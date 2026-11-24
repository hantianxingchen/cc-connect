package core

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 卡片开关（core.CardSwitcher）：平台实现了 CardSender，但可经配置关闭卡片
// ---------------------------------------------------------------------------

// stubSwitchableCardPlatform 是实现了 CardSender、但可按配置关闭卡片的平台桩。
type stubSwitchableCardPlatform struct {
	stubCardPlatform
	cardsEnabled bool
}

func (p *stubSwitchableCardPlatform) CardsEnabled() bool { return p.cardsEnabled }

func newSwitchableCardPlatform(cardsEnabled bool) *stubSwitchableCardPlatform {
	return &stubSwitchableCardPlatform{
		stubCardPlatform: stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "wecom"}},
		cardsEnabled:     cardsEnabled,
	}
}

// 平台实现了 CardSender 但配置关闭卡片时，engine 必须把它当纯文本平台
func TestSupportsCards_RespectsCardSwitcher(t *testing.T) {
	p := newSwitchableCardPlatform(false)
	if supportsCards(p) {
		t.Fatal("cards disabled: supportsCards = true, want false")
	}

	p.cardsEnabled = true
	if !supportsCards(p) {
		t.Fatal("cards enabled: supportsCards = false, want true")
	}
}

// 关闭卡片：权限提示回落纯文本，与不实现 CardSender 的平台行为一致
func TestSendPermissionPrompt_CardSwitcherOffFallsBackToText(t *testing.T) {
	e := newTestEngine()
	p := newSwitchableCardPlatform(false)

	e.sendPermissionPrompt(p, "ctx", "full prompt text", "write_file", "/tmp/test.txt")

	if got := len(p.sentCards); got != 0 {
		t.Fatalf("cards disabled: sent %d cards, want 0", got)
	}
	sent := p.getSent()
	if len(sent) != 1 || sent[0] != "full prompt text" {
		t.Fatalf("cards disabled: sent = %q, want the plain prompt text", sent)
	}
}

// 开启卡片：保持上游行为，权限提示走 CardSender
func TestSendPermissionPrompt_CardSwitcherOnKeepsCard(t *testing.T) {
	e := newTestEngine()
	p := newSwitchableCardPlatform(true)

	e.sendPermissionPrompt(p, "ctx", "full prompt text", "write_file", "/tmp/test.txt")

	if got := len(p.sentCards); got != 1 {
		t.Fatalf("cards enabled: sent %d cards, want 1", got)
	}
}

// 关闭卡片：卡片回复入口回落文本渲染
func TestReplyWithCard_CardSwitcherOffFallsBackToText(t *testing.T) {
	e := newTestEngine()
	p := newSwitchableCardPlatform(false)

	e.replyWithCard(p, "ctx", NewCard().Title("标题", "blue").Markdown("card body text").Build())

	if got := len(p.repliedCards); got != 0 {
		t.Fatalf("cards disabled: replied %d cards, want 0", got)
	}
	sent := p.getSent()
	if len(sent) != 1 || !strings.Contains(sent[0], "card body text") {
		t.Fatalf("cards disabled: sent = %q, want the text-rendered card body", sent)
	}
}

// 关闭卡片：卡片主动发送入口回落文本渲染
func TestSendWithCard_CardSwitcherOffFallsBackToText(t *testing.T) {
	e := newTestEngine()
	p := newSwitchableCardPlatform(false)

	e.sendWithCard(p, "ctx", NewCard().Title("标题", "blue").Markdown("card body text").Build())

	if got := len(p.sentCards); got != 0 {
		t.Fatalf("cards disabled: sent %d cards, want 0", got)
	}
	sent := p.getSent()
	if len(sent) != 1 || !strings.Contains(sent[0], "card body text") {
		t.Fatalf("cards disabled: sent = %q, want the text-rendered card body", sent)
	}
}
