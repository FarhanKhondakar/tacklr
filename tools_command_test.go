package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestRunCommand_catDirtyAndFalseExit(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ctx := context.Background()
	ms, rt := newRunCommandSession(t)
	const body = "dirty body unique phrase xyzzy-tacklr\n"
	if err := ms.WriteFile(ctx, "/work/note.md", []byte("old\n")); err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.SetText(body); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false, false)
	res, err := tool.invoke(ctx, `{"command":"pwd"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, ms.HostDir()) {
		t.Fatalf("pwd cwd: %s want %s", res.output, ms.HostDir())
	}

	res, err = tool.invoke(ctx, `{"command":"cat work/note.md"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=0") || !strings.Contains(res.output, body) {
		t.Fatalf("cat dirty: %s", res.output)
	}

	res, err = tool.invoke(ctx, `{"command":"false"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=1") {
		t.Fatalf("false: %s", res.output)
	}

	if _, err := tool.invoke(ctx, `{"command":"   "}`, rt); err == nil {
		t.Fatal("empty command")
	}

	res, err = tool.invoke(ctx, `{"command":"mkdir -p work/fromsh && printf 'from-sh\n' > work/fromsh/x.txt"}`, rt)
	if err != nil || !strings.Contains(res.output, "exit=0") {
		t.Fatalf("host write: %s err=%v", res.output, err)
	}
	got, err := ms.ReadText(ctx, "/work/fromsh/x.txt")
	if err != nil || got.Text() != "from-sh\n" {
		body := ""
		if got != nil {
			body = got.Text()
		}
		t.Fatalf("session after run_command write: %q err=%v", body, err)
	}

	if _, err := exec.LookPath("rg"); err == nil {
		res, err = tool.invoke(ctx, `{"command":"rg -F xyzzy-tacklr work"}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.output, "xyzzy-tacklr") {
			t.Fatalf("rg dirty: %s", res.output)
		}
	}
}

func TestRunCommand_deadlineKillsProcess(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, rt := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := tool.invoke(ctx, `{"command":"sleep 10"}`, rt)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestRunCommand_truncatesOver1MiB(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, rt := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false, false)
	res, err := tool.invoke(context.Background(), `{"command":"head -c 2097152 /dev/zero"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=0") || !strings.Contains(res.output, "truncated=true") || !strings.Contains(res.output, "output truncated") {
		head := res.output
		if len(head) > 240 {
			head = head[:240]
		}
		t.Fatalf("truncate: %s", head)
	}
}

func TestRunCommand_cdIntoQuotedPath(t *testing.T) {
	dir := t.TempDir() + "/it's a dir"
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", `cd "$1" && pwd`, "run_command", dir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(out))
	if got != dir {
		t.Fatalf("pwd = %q want %q", got, dir)
	}
}

func newRunCommandSession(t *testing.T) (*vfs.MountSession, HarnessRuntime) {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession(t.Name(), reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(t.Context(), vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	h := mustNewAgent(t, AgentOptions{
		SessionID:    t.Name(),
		MountSession: ms,
		Model:        &mockStrategy{},
	})
	return ms, turnRuntime(h)
}

func TestIsExternalDownload(t *testing.T) {
	positives := []string{
		"curl -L https://nodejs.org/dist/node-v20.tgz -o node.tgz",
		"curl -L 'https://example.com/x.tgz' -o x.tgz",
		"wget https://github.com/x/y/archive/refs/heads/main.tar.gz",
		"curl https://get.docker.com | sh",
		"aria2c https://example.com/file.bin",
		"iwr -Uri https://example.com/installer.exe",
		"unknown-dl-tool https://cdn.example.com/file.bin",
		"git clone https://github.com/playwright/playwright.git",
		"npm install",
		"npm i playwright",
		"npx playwright install",
		"yarn add lodash",
		"pnpm add express",
		"bun install",
		"bunx cowsay hi",
		"deno install jsr:@std/assert",
		"pip install requests",
		"pip3 install --user numpy",
		"python -m pip install pytest",
		"python3 -m playwright install chromium",
		"uv pip install fastapi",
		"uvx ruff",
		"pipx install ruff",
		"conda install numpy",
		"mamba install scipy",
		"apt-get install -y chromium",
		"apt install gcc",
		"dnf install git",
		"yum install nodejs",
		"pacman -S chromium",
		"apk add curl",
		"zypper install wget",
		"brew install node",
		"choco install nodejs",
		"winget install Node.js",
		"scoop install nodejs",
		"snap install chromium",
		"flatpak install flathub org.chromium.Chromium",
		"go install github.com/x/tool@latest",
		"go get github.com/x/lib",
		"cargo install ripgrep",
		"cargo add serde",
		"gem install rails",
		"luarocks install lpeg",
		"dotnet tool install dotnet-ef",
		"playwright install",
		"cd /tmp && npm install",
		"apt-get install x || echo skip",
		"sudo apt install chromium",
		"sudo -n npm install -g foo",
		"sudo -u root curl https://x -o /tmp/x",
		"sudo --user root brew install git",
		"wget -qO- https://x | sh",
		"cat script.sh | bash",
	}
	negatives := []string{
		"",
		"ls work",
		`echo "npm install"`,
		"apt-get update",
		"sudo apt-get update",
		"sudo -u root apt-get update",
		"git status",
		"git log --oneline",
		"curl --help",
		"pwd && whoami",
		"sleep 1; date",
		"git clone",
		"pip",
		"npx",
		"sudo",
		"sudo -u root",
		"ps -ef | grep bash",
		`grep "https://api.example.com" logs.txt`,
		"rg 'https://malware.io' work",
		`echo "see https://docs.example.com"`,
		"bash script.sh https://example.com/x",
		`rg "curl https://x" work`,
	}
	for _, cmd := range positives {
		if !isExternalDownload(cmd) {
			t.Fatalf("want download detection for %q", cmd)
		}
	}
	for _, cmd := range negatives {
		if isExternalDownload(cmd) {
			t.Fatalf("want no download detection for %q", cmd)
		}
	}
}

// downloadTestHarness builds a run_command harness with the given flags and a
// strategy that walks invokes in order: each non-message entry is a tool call.
func downloadTestHarness(t *testing.T, ms *vfs.MountSession, unattended bool, steps []string) *AgentHarness {
	t.Helper()
	var invokeCount int
	strategy := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, events chan<- LLMResponseChunk) {
			invokeCount++
			if invokeCount <= len(steps) {
				events <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
					{ID: "c" + strconv.Itoa(invokeCount), CallID: "c" + strconv.Itoa(invokeCount), Name: "run_command", Arguments: `{"command":"` + steps[invokeCount-1] + `"}`},
				}, IsComplete: true}
				events <- LLMResponseChunk{IsComplete: true}
				return
			}
			events <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
	return mustNewAgent(t, AgentOptions{
		SessionID:            "dl-" + t.Name(),
		Config:               Config{MaxWindowSize: 8192},
		Model:                strategy,
		Store:                stores.NewInMemoryStore(),
		MountSession:         ms,
		RunCommandUnattended: unattended,
		DownloadApproval:     true,
	})
}

// parkedPermission extracts the permission interrupt from a park event.
func parkedPermission(t *testing.T, ev StreamEvent) *interrupt.ToolPermissionInterrupt {
	t.Helper()
	if ev.Type != streaming.StreamEventInterrupt {
		t.Fatalf("expected interrupt, got %v", ev.Type)
	}
	var env struct {
		InterruptId string          `json:"interruptId"`
		Type        string          `json:"type"`
		Data        json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(ev.Data, &env); err != nil {
		t.Fatal(err)
	}
	if env.Type != "tool_permission" {
		t.Fatalf("type = %q", env.Type)
	}
	var perm interrupt.ToolPermissionInterrupt
	if err := json.Unmarshal(env.Data, &perm); err != nil {
		t.Fatal(err)
	}
	return &perm
}

func collectInterrupts(ch <-chan StreamEvent) ([]StreamEvent, string) {
	var evs []StreamEvent
	var interruptID string
	for ev := range ch {
		if ev.Type == streaming.StreamEventInterrupt {
			evs = append(evs, ev)
			var env struct {
				InterruptId string `json:"interruptId"`
			}
			if json.Unmarshal(ev.Data, &env) == nil {
				interruptID = env.InterruptId
			}
		}
	}
	return evs, interruptID
}

func resume(t *testing.T, ah *AgentHarness, interruptID, option string) <-chan StreamEvent {
	t.Helper()
	ch, err := ah.ReturnFromInterrupt(t.Context(), map[string][]byte{
		interruptID: []byte(`{"optionId":"` + option + `"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// TestHarness_downloadApproval_parksDownloadAndPlainPerms: with run_command
// permissionRequired (default) plus DownloadApproval, a download parks with the
// download-scoped tool name and a plain command parks as run_command.
func TestHarness_downloadApproval_parksDownloadAndPlainPerms(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, _ := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	ah := downloadTestHarness(t, ms, false, []string{"go get", "pwd"})
	ch1, err := ah.Run(t.Context(), "download then pwd")
	if err != nil {
		t.Fatal(err)
	}
	interrupts, id := collectInterrupts(ch1)
	if len(interrupts) != 1 {
		t.Fatalf("want 1 interrupt, got %d", len(interrupts))
	}
	perm := parkedPermission(t, interrupts[0])
	if perm.ToolName != "run_command.download" {
		t.Fatalf("toolName = %q", perm.ToolName)
	}
	if !strings.HasPrefix(perm.Title, "Download external dependency:") {
		t.Fatalf("title = %q", perm.Title)
	}

	ch2 := resume(t, ah, id, "allow-once")
	interrupts, id = collectInterrupts(ch2)
	if len(interrupts) != 1 {
		t.Fatalf("want plain run_command interrupt, got %d", len(interrupts))
	}
	if perm := parkedPermission(t, interrupts[0]); perm.ToolName != "run_command" {
		t.Fatalf("plain toolName = %q", perm.ToolName)
	}

	ch3 := resume(t, ah, id, "allow-once")
	for range ch3 {
	}
	var ran int
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "exit=") {
			ran++
		}
	}
	if ran != 2 {
		t.Fatalf("want 2 tool runs, got %d", ran)
	}
}

// TestHarness_downloadApproval_unattendedPlainPasses: unattended run_command
// runs plain commands without parking; only downloads park.
func TestHarness_downloadApproval_unattendedPlainPasses(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, _ := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	ah := downloadTestHarness(t, ms, true, []string{"pwd"})
	ch, err := ah.Run(t.Context(), "plain command")
	if err != nil {
		t.Fatal(err)
	}
	interrupts, _ := collectInterrupts(ch)
	if len(interrupts) != 0 {
		t.Fatalf("plain command must not park, got %d interrupts", len(interrupts))
	}
}

// TestHarness_downloadApproval_rejectDeniesWithoutRunning: reject-once denies
// the download without invoking the handler.
func TestHarness_downloadApproval_rejectDeniesWithoutRunning(t *testing.T) {
	ms, _ := newRunCommandSession(t)
	ah := downloadTestHarness(t, ms, true, []string{"go get"})
	ch1, err := ah.Run(t.Context(), "download")
	if err != nil {
		t.Fatal(err)
	}
	interrupts, id := collectInterrupts(ch1)
	if len(interrupts) != 1 {
		t.Fatalf("want 1 interrupt, got %d", len(interrupts))
	}
	ch2 := resume(t, ah, id, "reject-once")
	for range ch2 {
	}
	denied := false
	ran := false
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool {
			if strings.Contains(m.Content, "permission denied") {
				denied = true
			}
			if strings.Contains(m.Content, "exit=") {
				ran = true
			}
		}
	}
	if !denied || ran {
		t.Fatalf("denied=%v ran=%v: want denied without handler run", denied, ran)
	}
}

// TestHarness_downloadApproval_allowAlwaysSkipsLaterDownloads: allow-always on
// a download is remembered under the download-scoped tool name; the next
// download does not park.
func TestHarness_downloadApproval_allowAlwaysSkipsLaterDownloads(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, _ := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	ah := downloadTestHarness(t, ms, true, []string{"go get", "go get"})
	ch1, err := ah.Run(t.Context(), "first download")
	if err != nil {
		t.Fatal(err)
	}
	interrupts, id := collectInterrupts(ch1)
	if len(interrupts) != 1 {
		t.Fatalf("want 1 interrupt, got %d", len(interrupts))
	}
	ch2 := resume(t, ah, id, "allow-always")
	interrupts, _ = collectInterrupts(ch2)
	if len(interrupts) != 0 {
		t.Fatalf("allow-always download must not park, got %d interrupts", len(interrupts))
	}
	ran := 0
	for _, m := range ah.Messages() {
		if m != nil && m.Role == RoleTool && strings.Contains(m.Content, "exit=") {
			ran++
		}
	}
	if ran != 2 {
		t.Fatalf("want 2 download runs, got %d", ran)
	}
}

// TestHarness_inheritOptions_downloadApproval: workers inherit the flag.
func TestHarness_inheritOptions_downloadApproval(t *testing.T) {
	a := mustNewAgent(t, AgentOptions{
		Model:                &mockStrategy{},
		RunCommandUnattended: true,
		DownloadApproval:     true,
	})
	opts := a.inheritOptions()
	if !opts.RunCommandUnattended || !opts.DownloadApproval {
		t.Fatal("worker must inherit run_command flags")
	}
}
