package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

const (
	wsEndpoint      = "wss://openws.work.weixin.qq.com"
	wsPingInterval  = 30 * time.Second
	wsMaxBackoff    = 30 * time.Second
	wsMaxMissedPong = 2
)

// WSPlatform implements core.Platform using the WeChat Work WebSocket long-connection
// mode (智能机器人长连接). No public URL, no message encryption, no IP allowlist required.
type WSPlatform struct {
	botID       string
	secret      string
	allowFrom   string
	wsURL       string // WebSocket endpoint; defaults to wsEndpoint (公有云), 可经 ws_url 覆盖为私有化部署地址
	conn        *websocket.Conn
	handler     core.MessageHandler
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex // protects conn writes
	dedup       core.MessageDedup
	reqSeq      atomic.Int64 // monotonic counter for generating unique req_id
	missedPong  atomic.Int32 // consecutive heartbeat acks not received
	pendingAcks sync.Map     // req_id -> chan wsAckResult, for sequential send with ack waiting

	// 群共享会话：群内所有人 @ bot 进同一个会话（sessionKey 去 userID 段），
	// 上下文互通；默认 false 保持每用户独立会话的旧行为
	groupSharedSessions bool
	// adminFrom 由 engine 注入（SetAdminFrom，core.AdminFromReceiver），
	// 未授权用户被 allowFrom 拦截时 proactive Send 通知这些管理员
	adminFrom string
	// 未授权通知去重：adminID|userID -> 上次通知时间；同一对 1 小时至多一条，
	// 防好奇用户连发刷屏管理员
	notifyMu   sync.Mutex
	lastNotify map[string]time.Time
	// allowFrom 运行时可变（/allowlist 命令热更新准入名单），allowMu 保护；
	// 与 conn 写锁（mu）分离避免读写名单阻塞消息发送
	allowMu sync.RWMutex
	// 企微官方 MCP 端点凭证与地址（N1 未授权通知把混淆 chat_id 翻译成
	// chat_name 用；mcpAPIKey 为空 = 翻译功能关闭，通知保持纯混淆 ID）
	mcpAPIKey  string
	mcpBaseURL string
}

const (
	wsAckTimeout      = 5 * time.Second
	wsMediaAckTimeout = 30 * time.Second
)

var errWSAckTimeout = errors.New("wecom-ws: ack timeout")

// wsReplyContext holds the context needed to reply to a specific message.
type wsReplyContext struct {
	reqID    string // req_id from headers of aibot_msg_callback
	chatID   string // chatid for aibot_send_msg
	chatType string // chattype: "single" or "group"
	userID   string // from.userid
}

// --- WebSocket protocol frame types (matching official SDK) ---

// wsFrame is the unified frame structure used for all WebSocket communication.
// Format: { cmd, headers: { req_id }, body: {...} }
// Response frames may omit cmd and include errcode/errmsg instead.
type wsFrame struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers wsFrameHeaders  `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode *int            `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

type wsFrameHeaders struct {
	ReqID string `json:"req_id"`
}

type wsAckResult struct {
	frame wsFrame
	err   error
}

// wsMsgCallbackBody is the body of an aibot_msg_callback frame.
type wsMsgCallbackBody struct {
	MsgID    string `json:"msgid"`
	AibotID  string `json:"aibotid"`
	ChatID   string `json:"chatid"`
	ChatType string `json:"chattype"` // "single" or "group"
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
	// Voice: official field is content; some payloads used text — accept both.
	Voice struct {
		Text    string `json:"text,omitempty"`
		Content string `json:"content,omitempty"`
	} `json:"voice"`
	Image *struct {
		URL    string `json:"url"`
		Aeskey string `json:"aeskey"`
	} `json:"image,omitempty"`
	File *struct {
		URL    string `json:"url"`
		Aeskey string `json:"aeskey"`
	} `json:"file,omitempty"`
	Mixed      *wsMixedBlock `json:"mixed,omitempty"`
	Quote      *wsQuoteBlock `json:"quote,omitempty"`
	CreateTime int64         `json:"create_time"`
}

func wsVoiceText(v struct {
	Text    string `json:"text,omitempty"`
	Content string `json:"content,omitempty"`
}) string {
	if s := strings.TrimSpace(v.Content); s != "" {
		return s
	}
	return strings.TrimSpace(v.Text)
}

func newWebSocket(opts map[string]any) (core.Platform, error) {
	botID, _ := opts["bot_id"].(string)
	secret, _ := opts["bot_secret"].(string)
	if botID == "" || secret == "" {
		return nil, fmt.Errorf("wecom-ws: bot_id and bot_secret are required for websocket mode")
	}
	allowFrom, _ := opts["allow_from"].(string)
	wsURL, _ := opts["ws_url"].(string)
	wsURL = strings.TrimSpace(wsURL)
	if wsURL == "" {
		wsURL = wsEndpoint
	}
	groupShared, _ := opts["group_shared_sessions"].(bool)
	mcpAPIKey, _ := opts["mcp_apikey"].(string)
	mcpBaseURL, _ := opts["mcp_url"].(string)

	return &WSPlatform{
		botID:               botID,
		secret:              secret,
		allowFrom:           allowFrom,
		wsURL:               wsURL,
		groupSharedSessions: groupShared,
		mcpAPIKey:           strings.TrimSpace(mcpAPIKey),
		mcpBaseURL:          strings.TrimSpace(mcpBaseURL),
	}, nil
}

// SetAdminFrom implements core.AdminFromReceiver. adminFrom 为逗号分隔的
// 管理员 userID 列表；engine 装配时注入，未授权访问通知按此列表推送。
func (p *WSPlatform) SetAdminFrom(adminFrom string) {
	p.notifyMu.Lock()
	p.adminFrom = strings.TrimSpace(adminFrom)
	p.notifyMu.Unlock()
}

// GetAllowFrom 返回当前准入名单（core.AllowFromManager，/allowlist 命令用）
func (p *WSPlatform) GetAllowFrom() string {
	p.allowMu.RLock()
	defer p.allowMu.RUnlock()
	return p.allowFrom
}

// SetAllowFrom 原子替换准入名单，下一条入站消息立即生效（/allowlist 热更新，
// 免 daemon restart——allow_from 不在 config reload 热加载清单内的缺口由
// engine 的 reloadConfig 补调本方法）
func (p *WSPlatform) SetAllowFrom(allowFrom string) {
	p.allowMu.Lock()
	p.allowFrom = strings.TrimSpace(allowFrom)
	p.allowMu.Unlock()
}

// generateReqID creates a unique req_id with the given prefix (e.g. "ping_1", "aibot_subscribe_2").
func (p *WSPlatform) generateReqID(prefix string) string {
	seq := p.reqSeq.Add(1)
	return fmt.Sprintf("%s_%d", prefix, seq)
}

func (p *WSPlatform) Name() string { return "wecom" }

func (p *WSPlatform) Start(handler core.MessageHandler) error {
	p.handler = handler
	p.ctx, p.cancel = context.WithCancel(context.Background())

	go p.connectLoop()
	return nil
}

// connectLoop establishes the WebSocket connection and reconnects on failure with
// exponential backoff (1s → 2s → 4s → ... → 30s max).
func (p *WSPlatform) connectLoop() {
	backoff := time.Second
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		start := time.Now()
		err := p.runConnection()
		if p.ctx.Err() != nil {
			return // shutting down
		}

		// If the connection was alive for a meaningful period, reset backoff
		if time.Since(start) > 2*wsPingInterval {
			backoff = time.Second
		}

		slog.Warn("wecom-ws: connection lost, reconnecting", "error", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-p.ctx.Done():
			return
		}

		backoff *= 2
		if backoff > wsMaxBackoff {
			backoff = wsMaxBackoff
		}
	}
}

// runConnection dials, subscribes, and processes messages until disconnection.
func (p *WSPlatform) runConnection() error {
	slog.Info("wecom-ws: connecting", "endpoint", p.wsURL)

	conn, _, err := websocket.DefaultDialer.DialContext(p.ctx, p.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.conn = nil
		p.mu.Unlock()
		conn.Close()

		// Drain pending ACK channels so waiting goroutines are unblocked
		// and stale entries do not accumulate across reconnections.
		// Collect keys first, then delete — Range+Delete in callback is
		// not guaranteed safe by the sync.Map contract.
		var staleKeys []any
		p.pendingAcks.Range(func(key, value any) bool {
			if ch, ok := value.(chan wsAckResult); ok {
				select {
				case ch <- wsAckResult{err: fmt.Errorf("wecom-ws: connection closed")}:
				default:
				}
			}
			staleKeys = append(staleKeys, key)
			return true
		})
		for _, k := range staleKeys {
			p.pendingAcks.Delete(k)
		}
	}()

	// Send subscribe (auth) frame
	// Format: { cmd: "aibot_subscribe", headers: { req_id }, body: { bot_id, secret } }
	subReqID := p.generateReqID("aibot_subscribe")
	subFrame := map[string]any{
		"cmd":     "aibot_subscribe",
		"headers": map[string]string{"req_id": subReqID},
		"body": map[string]string{
			"bot_id": p.botID,
			"secret": p.secret,
		},
	}
	if err := p.writeJSON(subFrame); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	// Read subscribe response: { headers: { req_id }, errcode: 0, errmsg: "ok" }
	var subResp wsFrame
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("subscribe response: %w", err)
	}
	if subResp.ErrCode == nil || *subResp.ErrCode != 0 {
		errCode := 0
		if subResp.ErrCode != nil {
			errCode = *subResp.ErrCode
		}
		return fmt.Errorf("subscribe failed: errcode=%d errmsg=%s", errCode, subResp.ErrMsg)
	}
	slog.Info("wecom-ws: subscribed successfully", "bot_id", p.botID)
	p.missedPong.Store(0)

	// Start heartbeat goroutine
	heartCtx, heartCancel := context.WithCancel(p.ctx)
	defer heartCancel()
	go p.heartbeat(heartCtx, conn)

	// Read loop
	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var frame wsFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			slog.Warn("wecom-ws: invalid json", "error", err)
			continue
		}

		p.handleFrame(frame)
	}
}

// handleFrame dispatches incoming frames by cmd or req_id prefix.
func (p *WSPlatform) handleFrame(frame wsFrame) {
	switch frame.Cmd {
	case "aibot_msg_callback":
		p.handleMsgCallback(frame)
	case "aibot_event_callback":
		slog.Debug("wecom-ws: event callback received (ignored)", "req_id", frame.Headers.ReqID)
	case "":
		// Response frame (no cmd): identify by req_id prefix
		reqID := frame.Headers.ReqID
		switch {
		case strings.HasPrefix(reqID, "ping"):
			p.missedPong.Store(0)
			slog.Debug("wecom-ws: heartbeat ack received")
		case strings.HasPrefix(reqID, "aibot_subscribe"):
			// Late subscribe ack (should have been consumed in runConnection)
			slog.Debug("wecom-ws: late subscribe ack")
		default:
			var ackErr error
			if frame.ErrCode != nil && *frame.ErrCode != 0 {
				ackErr = fmt.Errorf("wecom-ws: ack error: errcode=%d errmsg=%s", *frame.ErrCode, frame.ErrMsg)
				slog.Warn("wecom-ws: reply/send ack error", "req_id", reqID, "errcode", *frame.ErrCode, "errmsg", frame.ErrMsg)
			} else {
				slog.Debug("wecom-ws: reply/send ack ok", "req_id", reqID)
			}
			p.dispatchAck(reqID, wsAckResult{frame: frame, err: ackErr})
		}
	default:
		slog.Debug("wecom-ws: unhandled cmd", "cmd", frame.Cmd)
	}
}

func (p *WSPlatform) dispatchAck(reqID string, result wsAckResult) {
	ch, ok := p.pendingAcks.LoadAndDelete(reqID)
	if !ok {
		return
	}
	resultCh, ok := ch.(chan wsAckResult)
	if !ok {
		slog.Warn("wecom-ws: unexpected ack channel type", "req_id", reqID)
		return
	}
	resultCh <- result
}

func (p *WSPlatform) heartbeat(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			missed := int(p.missedPong.Load())
			if missed >= wsMaxMissedPong {
				slog.Warn("wecom-ws: no heartbeat ack for consecutive pings, connection considered dead",
					"missed", missed)
				conn.Close()
				return
			}

			p.missedPong.Add(1)
			pingFrame := map[string]any{
				"cmd":     "ping",
				"headers": map[string]string{"req_id": p.generateReqID("ping")},
			}
			if err := p.writeJSON(pingFrame); err != nil {
				slog.Warn("wecom-ws: ping failed", "error", err)
				return
			}
			slog.Debug("wecom-ws: ping sent", "missed_pong", p.missedPong.Load())
		}
	}
}

func (p *WSPlatform) handleMsgCallback(frame wsFrame) {
	var body wsMsgCallbackBody
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		slog.Warn("wecom-ws: parse msg_callback body failed", "error", err)
		return
	}

	reqID := frame.Headers.ReqID

	if p.dedup.IsDuplicate(body.MsgID) {
		slog.Debug("wecom-ws: skipping duplicate message", "msg_id", body.MsgID)
		return
	}

	if body.CreateTime > 0 {
		if core.IsOldMessage(time.Unix(body.CreateTime, 0)) {
			slog.Debug("wecom-ws: ignoring old message", "create_time", body.CreateTime)
			return
		}
	}

	chatID := body.ChatID
	if chatID == "" {
		chatID = body.From.UserID
	}
	rctx := wsReplyContext{
		reqID:    reqID,
		chatID:   chatID,
		chatType: body.ChatType,
		userID:   body.From.UserID,
	}

	// allowFrom 可被 /allowlist 运行时替换，经 allowMu 读快照再判断
	if !core.AllowList(p.GetAllowFrom(), body.From.UserID) {
		slog.Info("wecom-ws: message from unauthorized user", "user", body.From.UserID,
			"chat_type", body.ChatType, "msg_id", body.MsgID)
		// 回执带 userID：WS 模式协议只下发混淆 ID、不解析名字，
		// 混淆 ID 正是申请 allowFrom 需要的凭证，引导用户转交给管理员
		if err := p.Reply(context.Background(), rctx,
			fmt.Sprintf("%s\n\n您的申请 ID: %s", core.UnauthorizedAccessMessage, body.From.UserID)); err != nil {
			slog.Warn("wecom-ws: unauthorized reply failed", "error", err)
		}
		p.notifyUnauthorizedAdmins(&body)
		return
	}

	sessionKey := fmt.Sprintf("wecom:%s:%s", chatID, body.From.UserID)
	if p.groupSharedSessions && body.ChatType == "group" {
		// 群共享会话：第三段固定 "group"（保持三段格式，
		// sessionKeyToReplyContext 的 parts 切分与 ReconstructReplyCtx 兼容）。
		// 群内所有人 @ 进同一会话上下文互通；owner 过滤按 chatID 天然隔离群外。
		sessionKey = fmt.Sprintf("wecom:%s:group", chatID)
	}

	// WS mode does not provide display names; the protocol only carries userID.
	// Name resolution would require a separate HTTP API call with corpSecret,
	// which is unavailable in WebSocket-only mode.
	chatName := ""
	if body.ChatType == "group" {
		chatName = body.ChatID
	}

	current, quoted := wsCollectInboundParts(&body)
	quotedContent := formatWSQuotedContent(quoted)

	switch body.MsgType {
	case "voice":
		vt := stripWeComAtMentions(wsVoiceText(body.Voice), p.botID, body.AibotID)
		if vt != "" {
			current.content = append([]string{vt}, current.content...)
		}
		if len(current.content) == 0 && quotedContent == "" && !current.hasMedia() && !quoted.hasMedia() {
			slog.Debug("wecom-ws: voice message with empty transcription, ignoring")
			return
		}
		if current.hasMedia() || quoted.hasMedia() {
			slog.Info("wecom-ws: voice + media", "user", body.From.UserID, "images", len(current.images)+len(quoted.images), "files", len(current.files)+len(quoted.files))
			go p.deliverWSMediaInbound(&body, sessionKey, chatName, rctx, current, quoted, true)
			return
		}
		content := stripWeComAtMentions(strings.Join(current.content, "\n"), p.botID, body.AibotID)
		slog.Debug("wecom-ws: voice received (transcribed)", "user", body.From.UserID, "len", len(content))
		go p.handler(p, &core.Message{
			SessionKey: sessionKey, Platform: "wecom",
			MessageID: body.MsgID,
			UserID:    body.From.UserID, UserName: body.From.UserID,
			ChatName: chatName,
			Content:  content, ExtraContent: quotedContent, ReplyCtx: rctx, FromVoice: true,
		})
		return
	}

	if !current.hasMedia() && !quoted.hasMedia() {
		if len(current.content) == 0 && quotedContent == "" {
			slog.Warn("wecom-ws: no text or media in message", "msg_type", body.MsgType, "msg_id", body.MsgID)
			return
		}
		content := stripWeComAtMentions(strings.Join(current.content, "\n"), p.botID, body.AibotID)
		slog.Debug("wecom-ws: text received", "user", body.From.UserID, "len", len(content))
		go p.handler(p, &core.Message{
			SessionKey: sessionKey, Platform: "wecom",
			MessageID: body.MsgID,
			UserID:    body.From.UserID, UserName: body.From.UserID,
			ChatName: chatName,
			Content:  content, ExtraContent: quotedContent, ReplyCtx: rctx,
		})
		return
	}

	slog.Info("wecom-ws: media message", "msg_type", body.MsgType, "user", body.From.UserID,
		"images", len(current.images)+len(quoted.images), "files", len(current.files)+len(quoted.files), "text_parts", len(current.content))
	go p.deliverWSMediaInbound(&body, sessionKey, chatName, rctx, current, quoted, false)
}

// Reply sends a response message via aibot_respond_msg using the stream format.
// Uses the req_id from the original callback.
// The stream content field is a full-replacement (not incremental append), so we
// send the complete content in one frame with finish=true.
// Markdown is natively supported by the stream reply format.
// notifyUnauthorizedAdmins 把未授权访问事件 proactive Send 推送给 adminFrom
// 名单里的管理员（私聊场景 admin 的 userID 即其 chatID）。通知里混淆 ID
// 会先经企微 MCP sessions_list 翻译成 chat_name（拼音+真名），管理员可直接
// 识别摸门者，混淆 ID 原样保留作为 /allowlist add 的凭证。同一 adminID|userID
// 对 1 小时内至多一条通知，防止好奇用户连发刷屏。翻译与发送全程异步，
// 失败只记日志，不影响拦截主流程。
func (p *WSPlatform) notifyUnauthorizedAdmins(body *wsMsgCallbackBody) {
	admins := strings.Split(p.adminFrom, ",")
	if len(admins) == 0 {
		return
	}
	now := time.Now()
	// 先过去重收集本轮要通知的管理员，全被去重则不做任何翻译/发送
	var targets []string
	for _, admin := range admins {
		admin = strings.TrimSpace(admin)
		// "*" 全员 admin 时无具体通知目标；被拦者本人是 admin 不会发生（在 allow_from 内）
		if admin == "" || admin == "*" || admin == body.From.UserID {
			continue
		}
		dedupKey := admin + "|" + body.From.UserID
		p.notifyMu.Lock()
		if t, ok := p.lastNotify[dedupKey]; ok && now.Sub(t) < time.Hour {
			p.notifyMu.Unlock()
			continue
		}
		if p.lastNotify == nil {
			p.lastNotify = make(map[string]time.Time)
		}
		p.lastNotify[dedupKey] = now
		p.notifyMu.Unlock()
		targets = append(targets, admin)
	}
	if len(targets) == 0 {
		return
	}

	source := "私聊"
	if body.ChatType == "group" {
		source = fmt.Sprintf("群聊（%s）", body.ChatID)
	}
	// 异步翻译+发送：sessions_list 查询最坏 1.5s、Send 最坏等 ack 超时（5s/管理员），
	// 都不该阻塞拦截主流程；翻译失败降级纯混淆 ID，通知照发。
	// 私聊场景 chatID 回落到 userID（见 handleMsgCallback），
	// 故直接用 admin userID 构造发送上下文
	go func() {
		chatName, ok := p.resolveUser(context.Background(), body.From.UserID)
		who := userLabel(body.From.UserID, chatName, ok)
		content := fmt.Sprintf("⚠️ 未授权访问\n用户：%s\n来源：%s\n时间：%s\n内容：「%s」",
			who, source, now.Format("2006-01-02 15:04"),
			truncRunes(body.Text.Content, 80))
		for _, admin := range targets {
			if err := p.Send(context.Background(), wsReplyContext{chatID: admin}, content); err != nil {
				slog.Warn("wecom-ws: unauthorized access notify failed", "admin", admin, "error", err)
			}
		}
	}()
}

// truncRunes 按 rune 截断字符串，超长补省略号（通知内容预览用）。
func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func (p *WSPlatform) Reply(ctx context.Context, rctx any, content string) error {
	rc, ok := rctx.(wsReplyContext)
	if !ok {
		return fmt.Errorf("wecom-ws: invalid reply context type %T", rctx)
	}
	if content == "" {
		return nil
	}

	streamID := p.generateReqID("stream")
	frame := map[string]any{
		"cmd":     "aibot_respond_msg",
		"headers": map[string]string{"req_id": rc.reqID},
		"body": map[string]any{
			"msgtype": "stream",
			"stream": map[string]any{
				"id":      streamID,
				"finish":  true,
				"content": content,
			},
		},
	}
	if err := p.writeJSON(frame); err != nil {
		slog.Error("wecom-ws: reply failed", "user", rc.userID, "error", err)
		return err
	}
	slog.Debug("wecom-ws: reply sent", "user", rc.userID, "len", len(content))
	return nil
}

// Send sends a proactive message via aibot_send_msg (markdown format).
// Used for follow-up messages and cron-triggered messages where no req_id is available.
// Markdown is natively supported.
func (p *WSPlatform) Send(ctx context.Context, rctx any, content string) error {
	rc, ok := rctx.(wsReplyContext)
	if !ok {
		return fmt.Errorf("wecom-ws: invalid reply context type %T", rctx)
	}
	if content == "" {
		return nil
	}
	if rc.chatID == "" {
		return fmt.Errorf("wecom-ws: chatID is empty, cannot send proactive message")
	}

	chunks := splitByBytes(content, 2000)
	for i, chunk := range chunks {
		reqID := p.generateReqID("aibot_send_msg")
		frame := map[string]any{
			"cmd":     "aibot_send_msg",
			"headers": map[string]string{"req_id": reqID},
			"body": map[string]any{
				"chatid":  rc.chatID,
				"msgtype": "markdown",
				"markdown": map[string]string{
					"content": chunk,
				},
			},
		}
		if err := p.writeAndWaitAck(ctx, frame, reqID); err != nil {
			slog.Error("wecom-ws: send failed", "user", rc.userID, "chunk", i, "error", err)
			return err
		}
	}
	slog.Debug("wecom-ws: message sent", "user", rc.userID, "chunks", len(chunks), "total_len", len(content))
	return nil
}

// ReconstructReplyCtx rebuilds a reply context from a session key.
// Session key format: "wecom:{chatID}:{userID}".
// The reconstructed context has no req_id, so Reply() (which needs req_id for
// aibot_respond_msg) won't work — the engine should use Send() (aibot_send_msg)
// for cron/relay scenarios.
func (p *WSPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	// wecom:{chatID}:{userID}
	parts := strings.SplitN(sessionKey, ":", 3)
	if len(parts) < 3 || parts[0] != "wecom" {
		return nil, fmt.Errorf("wecom-ws: invalid session key %q", sessionKey)
	}
	return wsReplyContext{chatID: parts[1], userID: parts[2]}, nil
}

func (p *WSPlatform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// writeJSON sends a JSON message over the WebSocket connection with mutex protection.
func (p *WSPlatform) writeJSON(v any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return fmt.Errorf("wecom-ws: not connected")
	}
	return p.conn.WriteJSON(v)
}

// writeAndWaitAck sends a frame and waits for the server ack before returning.
// Falls back to non-blocking on timeout to avoid deadlocks.
func (p *WSPlatform) writeAndWaitAck(ctx context.Context, frame map[string]any, reqID string) error {
	return p.writeAndWaitAckWithTimeout(ctx, frame, reqID, wsAckTimeout)
}

func (p *WSPlatform) writeAndWaitAckWithTimeout(ctx context.Context, frame map[string]any, reqID string, timeout time.Duration) error {
	result, err := p.writeAndWaitResult(ctx, frame, reqID, timeout)
	if errors.Is(err, errWSAckTimeout) {
		slog.Debug("wecom-ws: ack timeout, proceeding", "req_id", reqID)
		return nil
	}
	if err != nil {
		return err
	}
	return result.err
}

func (p *WSPlatform) writeAndWaitAckStrict(ctx context.Context, frame map[string]any, reqID string, timeout time.Duration) error {
	result, err := p.writeAndWaitResult(ctx, frame, reqID, timeout)
	if errors.Is(err, errWSAckTimeout) {
		return fmt.Errorf("wecom-ws: ack timeout waiting for %s", reqID)
	}
	if err != nil {
		return err
	}
	return result.err
}

func (p *WSPlatform) writeAndWaitFrameWithTimeout(ctx context.Context, frame map[string]any, reqID string, timeout time.Duration) (wsFrame, error) {
	result, err := p.writeAndWaitResult(ctx, frame, reqID, timeout)
	if errors.Is(err, errWSAckTimeout) {
		return wsFrame{}, fmt.Errorf("wecom-ws: ack timeout waiting for %s", reqID)
	}
	if err != nil {
		return wsFrame{}, err
	}
	if result.err != nil {
		return wsFrame{}, result.err
	}
	return result.frame, nil
}

func (p *WSPlatform) writeAndWaitResult(ctx context.Context, frame map[string]any, reqID string, timeout time.Duration) (wsAckResult, error) {
	ch := make(chan wsAckResult, 1)
	p.pendingAcks.Store(reqID, ch)

	if err := p.writeJSON(frame); err != nil {
		p.pendingAcks.Delete(reqID)
		return wsAckResult{}, err
	}

	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		p.pendingAcks.Delete(reqID)
		return wsAckResult{}, ctx.Err()
	case <-time.After(timeout):
		p.pendingAcks.Delete(reqID)
		return wsAckResult{}, errWSAckTimeout
	}
}
