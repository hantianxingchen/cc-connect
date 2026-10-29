package wecom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 企微官方 MCP 端点（智能机器人消息域）：N1 未授权通知用它把混淆 chat_id
// 翻译成 chat_name（拼音账号+真名，如 "dorazhou(周先花)"），管理员收到通知
// 即可识别摸门者。协议为 MCP JSON-RPC over HTTP，apikey 走 URL query 参数；
// 无需 initialize 握手，可直接 tools/call。Accept 头必须含 application/json，
// 否则端点返回 -32600 Not Acceptable。
const (
	defaultMcpBaseURL   = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg"
	mcpCallTimeout      = 1500 * time.Millisecond
	mcpSessionsListTool = "message_aibot_sessions_list"
)

// mcpToolsCallRequest 是 tools/call 的 JSON-RPC 请求体（固定调 sessions_list）
type mcpToolsCallRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  mcpCallParams `json:"params"`
}

type mcpCallParams struct {
	Name      string   `json:"name"`
	Arguments struct{} `json:"arguments"`
}

// mcpToolsCallResponse 是 JSON-RPC 响应外层；工具结果藏在 result.content[0].text
// 里且是再嵌一层的 JSON 字符串（双层解析）
type mcpToolsCallResponse struct {
	Result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	} `json:"result"`
}

// mcpSessionEntry 是 sessions_list 返回的单条会话
type mcpSessionEntry struct {
	ChatID   string `json:"chat_id"`
	ChatName string `json:"chat_name"`
	ChatType string `json:"chat_type"`
	LastMsg  string `json:"last_msg_time"`
}

// mcpSessionsPayload 是 content[0].text 解开后的会话列表载荷
type mcpSessionsPayload struct {
	Sessions []mcpSessionEntry `json:"sessions"`
	ErrCode  int               `json:"errcode"`
	ErrMsg   string            `json:"errmsg"`
}

// resolveUser 通过 MCP sessions_list 把混淆 chat_id 翻译成 chat_name。
// 端点只返回最近 20 个会话，但 N1 触发时摸门者刚发过消息（且会收到拦截回执），
// 其会话必然最新鲜，窗口内必有。每次成功响应把整个列表 merge 进本地映射
// 文件（一次调用攒 20 条，快速积累成完整通讯簿）；MCP 未命中/失败时回退查
// 文件（覆盖掉窗场景：三周前来过、已掉出最近 20 的人再摸门）。
// 全链路失败（未配置 apikey / 超时 / 文件也没有）返回 false，
// 调用方降级为纯混淆 ID，不影响通知发送。
func (p *WSPlatform) resolveUser(ctx context.Context, chatID string) (string, bool) {
	if name, ok := p.resolveUserViaMCP(ctx, chatID); ok {
		return name, true
	}
	return lookupUserMapFile(chatID)
}

// resolveUserViaMCP 调 MCP sessions_list；响应有效时先全量 merge 落盘，
// 再在列表内匹配 chatID
func (p *WSPlatform) resolveUserViaMCP(ctx context.Context, chatID string) (string, bool) {
	if p.mcpAPIKey == "" {
		return "", false
	}
	baseURL := p.mcpBaseURL
	if baseURL == "" {
		baseURL = defaultMcpBaseURL
	}
	q := url.Values{}
	q.Set("apikey", p.mcpAPIKey)
	endpoint := baseURL + "?" + q.Encode()

	ctx, cancel := context.WithTimeout(ctx, mcpCallTimeout)
	defer cancel()

	reqBody, err := json.Marshal(mcpToolsCallRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  mcpCallParams{Name: mcpSessionsListTool},
	})
	if err != nil {
		return "", false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(reqBody)))
	if err != nil {
		return "", false
	}
	req.Header.Set("Content-Type", "application/json")
	// 端点强制要求 Accept 含 application/json，缺省会被 -32600 拒绝
	req.Header.Set("Accept", "application/json, text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return "", false
	}

	var outer mcpToolsCallResponse
	if err := json.Unmarshal(raw, &outer); err != nil || outer.Result.IsError || len(outer.Result.Content) == 0 {
		return "", false
	}
	var payload mcpSessionsPayload
	if err := json.Unmarshal([]byte(outer.Result.Content[0].Text), &payload); err != nil || payload.ErrCode != 0 {
		return "", false
	}
	mergeUserMap(payload.Sessions)
	for _, s := range payload.Sessions {
		if s.ChatID == chatID && strings.TrimSpace(s.ChatName) != "" {
			return s.ChatName, true
		}
	}
	return "", false
}

// userLabel 生成通知里"用户"行的显示：翻译成功为 "混淆ID（chat_name）"，
// 失败保持纯混淆 ID（它是 /allowlist add 的实际凭证，必须原样保留）。
// 空白名出口侧再防一层（resolveUser 已过滤，此处兜底文案出口）
func userLabel(chatID, chatName string, ok bool) string {
	if !ok || strings.TrimSpace(chatName) == "" {
		return chatID
	}
	return fmt.Sprintf("%s（%s）", chatID, chatName)
}

// --- 本地映射文件（~/.cc-connect/usermap.json）---
// 每次 MCP 响应全量 merge（一次调用攒整个近 20 列表），MCP 掉线/掉窗时
// 回退查文件。文件含真名+混淆 ID，敏感度等同 config.toml：0600、不进 git。

// userMapEntry 是映射文件的单条记录；混淆 ID per-bot 稳定、姓名不变，
// 映射只增不失效，last_msg 取端点给的最近消息时间（比本地时钟更有信息量）
type userMapEntry struct {
	Name    string `json:"name"`
	LastMsg string `json:"last_msg,omitempty"`
	Type    string `json:"type,omitempty"`
}

var userMapMu sync.Mutex

func userMapPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".cc-connect", "usermap.json"), nil
}

// mergeUserMap 把 MCP 响应里的会话列表合入本地映射文件（读-合-写）。
// 任一步失败只告警不返回错误：落盘是尽力而为的增强，不能影响翻译主链路
func mergeUserMap(sessions []mcpSessionEntry) {
	if len(sessions) == 0 {
		return
	}
	userMapMu.Lock()
	defer userMapMu.Unlock()

	path, err := userMapPath()
	if err != nil {
		slog.Warn("wecom-ws: usermap path unavailable", "error", err)
		return
	}
	entries := map[string]userMapEntry{}
	if raw, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(raw, &entries) // 文件损坏则从空重建，不视为错误
	}
	for _, s := range sessions {
		if name := strings.TrimSpace(s.ChatName); name != "" && s.ChatID != "" {
			entries[s.ChatID] = userMapEntry{Name: name, LastMsg: s.LastMsg, Type: s.ChatType}
		}
	}
	raw, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		slog.Warn("wecom-ws: usermap marshal failed", "error", err)
		return
	}
	// 生产环境 ~/.cc-connect 必在（daemon 工作目录），MkdirAll 兜底首次运行/
	// HOME 被重定向的场景
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		slog.Warn("wecom-ws: usermap mkdir failed", "error", err)
		return
	}
	// 先写临时文件再原子替换：写到一半崩溃/并发读不会拿到半个 JSON
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		slog.Warn("wecom-ws: usermap write failed", "error", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		slog.Warn("wecom-ws: usermap rename failed", "error", err)
		_ = os.Remove(tmp)
	}
}

// lookupUserMapFile 从本地映射查名（MCP 失败/掉窗时的回退路径）
func lookupUserMapFile(chatID string) (string, bool) {
	userMapMu.Lock()
	defer userMapMu.Unlock()

	path, err := userMapPath()
	if err != nil {
		return "", false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	entries := map[string]userMapEntry{}
	if err := json.Unmarshal(raw, &entries); err != nil {
		return "", false
	}
	e, ok := entries[chatID]
	if !ok || strings.TrimSpace(e.Name) == "" {
		return "", false
	}
	return e.Name, true
}
