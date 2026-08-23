package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/command"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	runCommandTimeout = 60 * time.Second

	// downloadPermissionToolName keys allow-always / reject-always memory
	// for download-detected commands separately from plain run_command.
	downloadPermissionToolName = "run_command.download"

	downloadTitleMaxLen = 120
)

type runCommandArgs struct {
	Command string `json:"command" desc:"Host shell command. Runs as /bin/sh -c. cwd is the VFS root. Use relative paths (work/foo). Absolute /work is the host /work until a later jail."`
}

// runCommandOnCall parks tool_permission for run_command. downloadApproval
// narrows the park to external-dependency downloads (distinct permission
// memory via downloadPermissionToolName); otherwise plain commands park only
// when permissionRequired. Returns nil to skip the layer.
func runCommandOnCall(permissionRequired, downloadApproval bool) OnCallFunc {
	return func(inv ToolInvocation) Interrupt {
		if downloadApproval {
			var args runCommandArgs
			if err := json.Unmarshal([]byte(inv.ArgsJSON), &args); err == nil {
				cmd := strings.TrimSpace(args.Command)
				if isExternalDownload(cmd) {
					title := cmd
					if r := []rune(title); len(r) > downloadTitleMaxLen {
						title = string(r[:downloadTitleMaxLen]) + "…"
					}
					return &interrupt.ToolPermissionInterrupt{
						ToolName: downloadPermissionToolName,
						Title:    "Download external dependency: " + title,
						Options:  interrupt.DefaultPermissionOptions(),
					}
				}
			}
		}
		if !permissionRequired {
			return nil
		}
		return ToolPermissionOnCall(inv)
	}
}

func newRunCommand(ms *vfs.MountSession, permissionRequired, downloadApproval bool) *Tool {
	cfg := ToolConfig{
		Name:        "run_command",
		DisplayName: "Run {command}",
		Description: `Run a host shell command as /bin/sh -c. cwd is the VFS root (FUSE mount). Use relative paths (work/foo, ./work/foo). Absolute /work is the host /work until a later jail. Non-zero exit is a successful tool result (exit=N).`,
		Category:    streaming.ToolCategoryExecute,
		Access:      ToolExecuteAccess,
		Timeout:     runCommandTimeout,
		Handler: func(ctx context.Context, args runCommandArgs, rt HarnessRuntime) (string, error) {
			dir := ms.HostDir()
			if dir == "" {
				return "", vfs.ErrFuseNotMounted
			}
			cmdStr := strings.TrimSpace(args.Command)
			if cmdStr == "" {
				return "", fmt.Errorf("run_command: command is required: %w", ErrInvalid)
			}
			rt.EmitUpdate("Running " + cmdStr)
			return command.Run(ctx, dir, cmdStr)
		},
	}
	if permissionRequired || downloadApproval {
		cfg.OnCall = []OnCallFunc{runCommandOnCall(permissionRequired, downloadApproval)}
	}
	return NewTool(cfg)
}

// isExternalDownload reports whether cmd downloads an external dependency:
// a transfer tool pulling an http(s) URL, git clone, a package-manager
// install, or any command piping into a shell. Statements are split on &&, ||,
// ;, |, and newlines; only the first token of each statement is checked, so
// echo/comment noise does not match.
func isExternalDownload(cmd string) bool {
	if pipesToShell(cmd) {
		return true
	}
	return slices.ContainsFunc(splitStatements(cmd), downloadStatement)
}

func splitStatements(cmd string) []string {
	return strings.FieldsFunc(cmd, func(r rune) bool {
		delimiters := []rune{'&', '|', ';', '\n'}
		return slices.Contains(delimiters, r)
	})
}

// pipeShells are sinks that execute piped content.
var pipeShells = map[string]struct{}{
	"sh": {}, "bash": {}, "zsh": {}, "dash": {}, "ash": {}, "fish": {},
	"ksh": {}, "csh": {}, "tcsh": {},
}

// pipesToShell reports whether any pipe segment executes via a shell,
// regardless of the producer (downloaded content or otherwise).
func pipesToShell(cmd string) bool {
	segs := strings.FieldsFunc(cmd, func(r rune) bool { return r == '|' })
	if len(segs) < 2 { // need at least one pipe for a sink to exist
		return false
	}
	for _, seg := range segs[1:] { // skip first segment — it is never a pipe sink
		f := strings.Fields(seg)
		if len(f) > 0 {
			if _, ok := pipeShells[strings.ToLower(f[0])]; ok {
				return true
			}
		}
	}
	return false
}

// statementRule describes download behavior for one command word.
type statementRule struct {
	// any flags every invocation.
	any bool
	// hasURL flags invocations that carry an http(s) URL argument.
	hasURL bool
	// prefix matches ordered token sequences after the command word.
	prefix [][]string
}

var statementRules = map[string]statementRule{
	"curl": {hasURL: true}, "curl.exe": {hasURL: true}, "wget": {hasURL: true},
	"aria2c": {hasURL: true}, "axel": {hasURL: true}, "iwr": {hasURL: true},
	"invoke-webrequest": {hasURL: true},

	"git":        {hasURL: true},
	"npm":        {prefix: [][]string{{"install"}, {"i"}, {"ci"}}},
	"yarn":       {prefix: [][]string{{"add"}}},
	"pnpm":       {prefix: [][]string{{"add"}, {"install"}, {"i"}}},
	"bun":        {prefix: [][]string{{"add"}, {"install"}, {"i"}}},
	"deno":       {prefix: [][]string{{"install"}}},
	"pip":        {prefix: [][]string{{"install"}}},
	"pip3":       {prefix: [][]string{{"install"}}},
	"pipx":       {prefix: [][]string{{"install"}}},
	"apt":        {prefix: [][]string{{"install"}}},
	"apt-get":    {prefix: [][]string{{"install"}}},
	"dnf":        {prefix: [][]string{{"install"}}},
	"yum":        {prefix: [][]string{{"install"}}},
	"zypper":     {prefix: [][]string{{"install"}}},
	"pacman":     {prefix: [][]string{{"-S"}, {"--sync"}}},
	"apk":        {prefix: [][]string{{"add"}}},
	"brew":       {prefix: [][]string{{"install"}}},
	"choco":      {prefix: [][]string{{"install"}}},
	"winget":     {prefix: [][]string{{"install"}}},
	"scoop":      {prefix: [][]string{{"install"}}},
	"snap":       {prefix: [][]string{{"install"}}},
	"flatpak":    {prefix: [][]string{{"install"}}},
	"go":         {prefix: [][]string{{"install"}, {"get"}}},
	"cargo":      {prefix: [][]string{{"install"}, {"add"}}},
	"gem":        {prefix: [][]string{{"install"}}},
	"luarocks":   {prefix: [][]string{{"install"}}},
	"conda":      {prefix: [][]string{{"install"}}},
	"mamba":      {prefix: [][]string{{"install"}}},
	"micromamba": {prefix: [][]string{{"install"}}},
	"dotnet":     {prefix: [][]string{{"tool", "install"}}},
	"python":     {prefix: [][]string{{"-m", "pip", "install"}, {"-m", "playwright", "install"}}},
	"python3":    {prefix: [][]string{{"-m", "pip", "install"}, {"-m", "playwright", "install"}}},
	"uv":         {prefix: [][]string{{"pip", "install"}}},
	"playwright": {prefix: [][]string{{"install"}}},

	"npx": {any: true}, "bunx": {any: true}, "uvx": {any: true},
}

// urlDenylist never triggers the URL fallback: these tools take text or files,
// not network fetches, so a URL argument is data, not a download.
var urlDenylist = map[string]struct{}{
	"grep": {}, "rg": {}, "sed": {}, "awk": {}, "cat": {}, "echo": {}, "printf": {},
	"less": {}, "head": {}, "tail": {}, "sort": {}, "uniq": {}, "wc": {}, "diff": {},
	"find": {}, "ls": {}, "pwd": {}, "mkdir": {}, "rm": {}, "cp": {}, "mv": {},
	"chmod": {}, "chown": {}, "ps": {}, "kill": {}, "date": {}, "uname": {}, "hostname": {},
	"which": {}, "xargs": {}, "cut": {}, "tr": {}, "tee": {}, "tar": {}, "gzip": {},
	"zip": {}, "unzip": {}, "test": {}, "true": {}, "false": {},
	"sh": {}, "bash": {}, "zsh": {}, "fish": {}, "dash": {},
}

func downloadStatement(stmt string) bool {
	fields := strings.Fields(stmt)
	if len(fields) == 0 {
		return false
	}
	fields = stripSudo(fields)
	if len(fields) == 0 {
		return false
	}
	tok := strings.ToLower(fields[0])
	if rule, ok := statementRules[tok]; ok {
		switch {
		case rule.any:
			return len(fields) > 1
		case rule.hasURL:
			return fieldHasURL(fields)
		default:
			return hasPrefix(fields[1:], rule.prefix)
		}
	}
	if _, denied := urlDenylist[tok]; denied {
		return false
	}
	return fieldHasURL(fields)
}

// stripSudo removes a sudo prefix (flags and -u user included) so the inner
// command is evaluated by the same rules as a plain invocation.
func stripSudo(fields []string) []string {
	if len(fields) == 0 || fields[0] != "sudo" {
		return fields
	}
	rest := fields[1:]
	for len(rest) > 0 {
		if slices.Contains([]string{"-u", "--user"}, rest[0]) {
			if len(rest) < 3 {
				return nil
			}
			rest = rest[2:]
			continue
		}
		if rest[0][0] == '-' {
			rest = rest[1:]
			continue
		}
		return rest
	}
	return nil
}

func fieldHasURL(fields []string) bool {
	return slices.ContainsFunc(fields, func(f string) bool {
		f = strings.TrimLeft(f, `"'`)
		return strings.HasPrefix(f, "http://") || strings.HasPrefix(f, "https://")
	})
}

// hasPrefix reports whether tail starts with any sequence in prefixes.
func hasPrefix(tail []string, prefixes [][]string) bool {
	return slices.ContainsFunc(prefixes, func(seq []string) bool {
		return len(tail) >= len(seq) && slices.Equal(tail[:len(seq)], seq)
	})
}
