package wecom

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// ---------------------------------------------------------------------------
// 企微卡片通道（上游 #1645）：开关 + 卡片按钮回调与群共享会话的适配
// ---------------------------------------------------------------------------

// wsCardEventFrame 构造 aibot_event_callback（template_card_event）帧。字段形态
// 取自企微官方文档「接收事件」：chattype 必填（single/group），chatid 仅群聊返回。
func wsCardEventFrame(t *testing.T, reqID, chatType, chatID, userID, eventKey string) wsFrame {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"msgid":       "msg_evt_" + reqID,
		"create_time": time.Now().Unix(),
		"aibotid":     "bot_123",
		"chattype":    chatType,
		"chatid":      chatID,
		"from":        map[string]any{"userid": userID},
		"msgtype":     "event",
		"event": map[string]any{
			"eventtype": "template_card_event",
			"event_key": eventKey,
			"task_id":   "task_abc",
		},
	})
	if err != nil {
		t.Fatalf("marshal event body: %v", err)
	}
	return wsFrame{
		Cmd:     "aibot_event_callback",
		Headers: wsFrameHeaders{ReqID: reqID},
		Body:    body,
	}
}

// 群共享会话下，卡片按钮回调必须落到 group 会话键：权限/提问卡片挂在
// wecom:<chat>:group 的 interactive state 上，上游按点击者拼出的
// wecom:<chat>:<user> 对不上号，会被引擎当过期回调丢弃（turn 挂到超时）。
func TestWSEventCallback_GroupSharedSessions_CardClickUsesSharedSessionKey(t *testing.T) {
	p, captured := newCapturedWSPlatform()
	p.groupSharedSessions = true

	p.handleEventCallback(wsCardEventFrame(t, "req_evt_g", "group", "group_chat_id_123", "zhangsan", "perm:allow"))

	select {
	case msg := <-captured:
		if msg.SessionKey != "wecom:group_chat_id_123:group" {
			t.Fatalf("group card click sessionKey = %q, want wecom:group_chat_id_123:group", msg.SessionKey)
		}
		// 点击者身份原样保留：engine 侧按 userID 判 admin / owner 归属依赖它
		if msg.UserID != "zhangsan" {
			t.Fatalf("UserID = %q, want zhangsan", msg.UserID)
		}
		if !msg.IsPermissionResponse || msg.Content != "allow" {
			t.Fatalf("permission response = (%q, %v), want (\"allow\", true)", msg.Content, msg.IsPermissionResponse)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

// 开关关闭（未启用群共享会话）时保持上游行为：按点击者隔离，每用户独立会话键
func TestWSEventCallback_GroupSharedSessions_OffKeepsPerUserKey(t *testing.T) {
	p, captured := newCapturedWSPlatform()

	p.handleEventCallback(wsCardEventFrame(t, "req_evt_off", "group", "group_chat_id_123", "zhangsan", "perm:allow"))

	select {
	case msg := <-captured:
		if msg.SessionKey != "wecom:group_chat_id_123:zhangsan" {
			t.Fatalf("sessionKey = %q, want wecom:group_chat_id_123:zhangsan", msg.SessionKey)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

// 私聊不受群共享影响：chatid 为空时回落 userID，会话键仍按人隔离
func TestWSEventCallback_GroupSharedSessions_PrivateChatUnaffected(t *testing.T) {
	p, captured := newCapturedWSPlatform()
	p.groupSharedSessions = true

	p.handleEventCallback(wsCardEventFrame(t, "req_evt_p", "single", "", "zhangsan", "perm:deny"))

	select {
	case msg := <-captured:
		if msg.SessionKey != "wecom:zhangsan:zhangsan" {
			t.Fatalf("private card click sessionKey = %q, want wecom:zhangsan:zhangsan", msg.SessionKey)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

// 企微卡片通道默认关闭：上游 #1645 的按钮回调只翻译 perm:，菜单类按钮（nav:/act:）
// 的点击会把原始动作串当用户消息喂给 agent。fork 默认按纯文本平台工作，行为与
// 引入卡片前一致；显式配置 enable_wecom_card = true 才开启（届时菜单类按钮仍不可用）。
func TestNewWebSocket_CardsDisabledByDefault(t *testing.T) {
	p, err := newWebSocket(map[string]any{"bot_id": "bot_123", "bot_secret": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if p.(core.CardSwitcher).CardsEnabled() {
		t.Fatal("cards enabled by default, want disabled (plain-text)")
	}
}

func TestNewWebSocket_CardsEnabledWhenOptedIn(t *testing.T) {
	p, err := newWebSocket(map[string]any{"bot_id": "bot_123", "bot_secret": "secret", "enable_wecom_card": true})
	if err != nil {
		t.Fatal(err)
	}
	if !p.(core.CardSwitcher).CardsEnabled() {
		t.Fatal("enable_wecom_card = true: cards still disabled")
	}
}

// 卡片回调与 /allowlist 热更新并发：准入名单必须经 allowMu 读快照。上游新增的
// 回调路径直接读 p.allowFrom，与 SetAllowFrom 的写构成数据竞争（-race 下必现）。
func TestWSEventCallback_AllowFromHotUpdateHasNoRace(t *testing.T) {
	p := &WSPlatform{
		allowFrom: "zhangsan",
		handler:   func(core.Platform, *core.Message) {},
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				p.SetAllowFrom("zhangsan,lisi")
			} else {
				p.SetAllowFrom("zhangsan")
			}
		}
	}()

	frame := wsCardEventFrame(t, "req_evt_race", "single", "", "zhangsan", "perm:allow")
	for i := 0; i < 200; i++ {
		p.handleEventCallback(frame)
	}
	close(stop)
	writer.Wait()
}
