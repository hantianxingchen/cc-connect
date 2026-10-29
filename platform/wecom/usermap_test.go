package wecom

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mcpServer 模拟企微官方 MCP 端点：校验 apikey query 与 Accept 头，
// 返回固定 sessions_list 载荷；resp 控制异常路径（空=正常响应）
func mcpServer(t *testing.T, resp string, hits *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apikey") == "" {
			http.Error(w, "missing apikey", http.StatusUnauthorized)
			return
		}
		if !strings.Contains(r.Header.Get("Accept"), "application/json") {
			http.Error(w, "not acceptable", http.StatusNotAcceptable)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		if hits != nil {
			*hits++
		}
		if resp == "" {
			resp = `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"sessions\":[{\"chat_id\":\"wohR_stranger\",\"chat_name\":\"dorazhou(周先花)\",\"chat_type\":\"single\",\"last_msg_time\":\"2026-10-29 17:02:29\"},{\"chat_id\":\"wr_group_1\",\"chat_name\":\"测试群\",\"chat_type\":\"group\"}],\"errcode\":0,\"errmsg\":\"ok\"}"}],"isError":false}}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(resp))
	}))
}

func TestResolveUser_MatchesChatIDToName(t *testing.T) {
	setUserMapHome(t)

	srv := mcpServer(t, "", nil)
	defer srv.Close()

	p := &WSPlatform{mcpAPIKey: "test-key", mcpBaseURL: srv.URL}
	name, ok := p.resolveUser(context.Background(), "wohR_stranger")
	if !ok || name != "dorazhou(周先花)" {
		t.Fatalf("resolveUser = %q, %v; want dorazhou(周先花), true", name, ok)
	}
}

func TestResolveUser_UnknownChatIDFallsBackFalse(t *testing.T) {
	setUserMapHome(t)

	srv := mcpServer(t, "", nil)
	defer srv.Close()

	p := &WSPlatform{mcpAPIKey: "test-key", mcpBaseURL: srv.URL}
	if _, ok := p.resolveUser(context.Background(), "wohR_nobody"); ok {
		t.Fatal("resolveUser should return false for chat_id not in session list")
	}
}

func TestResolveUser_NoAPIKeySkipsHTTP(t *testing.T) {
	setUserMapHome(t)

	hits := 0
	srv := mcpServer(t, "", &hits)
	defer srv.Close()

	p := &WSPlatform{mcpBaseURL: srv.URL} // mcpAPIKey 留空 = 功能关闭
	if _, ok := p.resolveUser(context.Background(), "wohR_stranger"); ok {
		t.Fatal("resolveUser should return false when apikey unconfigured")
	}
	if hits != 0 {
		t.Fatalf("no HTTP call expected when apikey unconfigured, got %d", hits)
	}
}

func TestResolveUser_ServerErrorFallsBackFalse(t *testing.T) {
	setUserMapHome(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := &WSPlatform{mcpAPIKey: "test-key", mcpBaseURL: srv.URL}
	if _, ok := p.resolveUser(context.Background(), "wohR_stranger"); ok {
		t.Fatal("resolveUser should return false on server error")
	}
}

func TestResolveUser_SlowServerTimesOut(t *testing.T) {
	setUserMapHome(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Second) // 超过 mcpCallTimeout=1.5s
	}))
	defer srv.Close()

	p := &WSPlatform{mcpAPIKey: "test-key", mcpBaseURL: srv.URL}
	start := time.Now()
	if _, ok := p.resolveUser(context.Background(), "wohR_stranger"); ok {
		t.Fatal("resolveUser should return false on timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("resolveUser should honor 1.5s timeout, took %v", elapsed)
	}
}

// setUserMapHome 把映射文件重定向到临时 HOME，测完还原
func setUserMapHome(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	return dir
}

func TestMergeUserMap_PersistsAndFallsBack(t *testing.T) {
	setUserMapHome(t)

	mergeUserMap([]mcpSessionEntry{
		{ChatID: "wohR_a", ChatName: "zhangsan(张三)", ChatType: "single", LastMsg: "2026-10-29 10:00:00"},
		{ChatID: "wohR_b", ChatName: "lisi(李四)", ChatType: "single"},
	})

	// 掉窗回退：文件里有而 MCP"窗口"外的 ID
	if name, ok := lookupUserMapFile("wohR_a"); !ok || name != "zhangsan(张三)" {
		t.Fatalf("lookupUserMapFile = %q, %v; want zhangsan(张三), true", name, ok)
	}
	if _, ok := lookupUserMapFile("wohR_missing"); ok {
		t.Fatal("lookupUserMapFile should miss unknown id")
	}

	// 二次 merge 保留旧条目并追加新条目
	mergeUserMap([]mcpSessionEntry{
		{ChatID: "wohR_c", ChatName: "wangwu(王五)"},
	})
	if _, ok := lookupUserMapFile("wohR_a"); !ok {
		t.Fatal("earlier entry should survive re-merge")
	}
	if _, ok := lookupUserMapFile("wohR_c"); !ok {
		t.Fatal("new entry should be merged in")
	}

	// 文件权限 0600（含真名，敏感度等同 config.toml）
	path, err := userMapPath()
	if err != nil {
		t.Fatalf("userMapPath: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat usermap: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("usermap perm = %o, want 600", perm)
	}
}

func TestResolveUser_MCPFailureFallsBackToFile(t *testing.T) {
	dir := setUserMapHome(t)
	// 预置映射文件（模拟历史积累，人已掉出 MCP 最近 20 窗口）
	mergeUserMap([]mcpSessionEntry{{ChatID: "wohR_old", ChatName: "zhaoliu(赵六)"}})

	// MCP 端点直接 500 → 走文件回退命中
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := &WSPlatform{mcpAPIKey: "test-key", mcpBaseURL: srv.URL}
	name, ok := p.resolveUser(context.Background(), "wohR_old")
	if !ok || name != "zhaoliu(赵六)" {
		t.Fatalf("resolveUser fallback = %q, %v; want zhaoliu(赵六), true", name, ok)
	}
	_ = dir
}

func TestUserLabel(t *testing.T) {
	if got := userLabel("wohR_x", "zhangsan(张三)", true); got != "wohR_x（zhangsan(张三)）" {
		t.Fatalf("userLabel translated = %q", got)
	}
	if got := userLabel("wohR_x", "", false); got != "wohR_x" {
		t.Fatalf("userLabel fallback = %q", got)
	}
	if got := userLabel("wohR_x", "  ", true); got != "wohR_x" {
		t.Fatalf("userLabel blank name should fall back, got %q", got)
	}
}

// TestNotifyUnauthorizedAdmins_NotificationCarriesRealName 验证 N1 通知文案
// 带翻译后的真名（混淆 ID 保留作为 /allowlist 凭证），MCP 挂掉时降级纯 ID 照发
func TestNotifyUnauthorizedAdmins_NotificationCarriesRealName(t *testing.T) {
	setUserMapHome(t)

	var mu sync.Mutex
	var sends []string

	upgrader := websocket.Upgrader{}
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			if cmd, _ := frame["cmd"].(string); cmd == "aibot_send_msg" {
				if body, ok := frame["body"].(map[string]any); ok {
					if md, ok := body["markdown"].(map[string]any); ok {
						if txt, ok := md["content"].(string); ok {
							mu.Lock()
							sends = append(sends, txt)
							mu.Unlock()
						}
					}
				}
			}
			headers, _ := frame["headers"].(map[string]any)
			ack := map[string]any{
				"headers": map[string]any{"req_id": headers["req_id"]},
				"errcode": 0,
				"errmsg":  "ok",
			}
			if err := conn.WriteJSON(ack); err != nil {
				return
			}
		}
	}))
	defer wsSrv.Close()

	wsURL := "ws" + wsSrv.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial test websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	mcpSrv := mcpServer(t, "", nil)
	defer mcpSrv.Close()

	p := &WSPlatform{allowFrom: "allowed-user", conn: conn, mcpAPIKey: "test-key", mcpBaseURL: mcpSrv.URL}
	p.SetAdminFrom("admin-1")
	go func() {
		for {
			var frame wsFrame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			p.handleFrame(frame)
		}
	}()

	body := wsMsgCallbackBody{MsgID: "m1", ChatType: "single", MsgType: "text"}
	body.From.UserID = "wohR_stranger"
	body.Text.Content = "在吗"
	body.CreateTime = time.Now().Unix()
	p.notifyUnauthorizedAdmins(&body)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(sends)
		mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification not sent, sends = %d", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	got := sends[0]
	mu.Unlock()
	if !strings.Contains(got, "wohR_stranger（dorazhou(周先花)）") {
		t.Fatalf("notification should carry translated name, got:\n%s", got)
	}
}
