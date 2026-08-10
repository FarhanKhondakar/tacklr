package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sync"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// DownloadGateConfig enables heuristic download detection on tool arguments.
// When Enabled, the downloadGate interceptor scans tool args for download-like
// patterns and raises a tool_permission interrupt when detected.
type DownloadGateConfig struct {
	Enabled     bool
	Patterns    []string
	ExemptTools []string
}

var defaultDownloadPatterns = []string{
	`npm\s+(i|install)`,
	`pip[23]?\s+install`,
	`yarn\s+(add|install)`,
	`pnpm\s+(add|install)`,
	`bun\s+(add|install)`,
	`apt-get\s+install`,
	`brew\s+install`,
	`cargo\s+(install|add)`,
	`go\s+(get|install)`,
	`wget\s+https?://`,
	`curl\s+.*-[oO]\s`,
	`git\s+clone`,
	`playwright\s+install`,
	`npx\s+playwright\s+install`,
	`https?://[^\s]+\.[a-z]{2,}`,
}

type downloadRegexps struct {
	compiled []*regexp.Regexp
}

var defaultDownloadRegexpsOnce sync.Once
var defaultDownloadRegexps downloadRegexps

func compileDownloadPatterns(patterns []string) downloadRegexps {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		r, err := regexp.Compile(p)
		if err != nil {
			continue
		}
		compiled = append(compiled, r)
	}
	return downloadRegexps{compiled}
}

func getDefaultDownloadRegexps() downloadRegexps {
	defaultDownloadRegexpsOnce.Do(func() {
		defaultDownloadRegexps = compileDownloadPatterns(defaultDownloadPatterns)
	})
	return defaultDownloadRegexps
}

func matchDownloadPatterns(regexps downloadRegexps, argsJSON string) string {
	if argsJSON == "" || argsJSON == "{}" {
		return ""
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return ""
	}
	return walkStrings(args, regexps)
}

func walkStrings(v any, d downloadRegexps) string {
	switch val := v.(type) {
	case string:
		for _, r := range d.compiled {
			if r.MatchString(val) {
				return r.String()
			}
		}
	case map[string]any:
		for _, sv := range val {
			if s := walkStrings(sv, d); s != "" {
				return s
			}
		}
	case []any:
		for _, sv := range val {
			if s := walkStrings(sv, d); s != "" {
				return s
			}
		}
	}
	return ""
}

const (
	downloadAlwaysAllowKey = "_download_always_allow"
	downloadAlwaysDenyKey  = "_download_always_deny"
)

func downloadGate(cfg *DownloadGateConfig) ToolInterceptor {
	return func(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
		if cfg == nil || !cfg.Enabled {
			return next(ctx, inv)
		}

		if inv.Tool == nil {
			return next(ctx, inv)
		}

		if inv.Tool.ApprovalReason != "" {
			return next(ctx, inv)
		}

		if slices.Contains(cfg.ExemptTools, inv.Tool.Name) {
			return next(ctx, inv)
		}

		name := inv.Tool.Name
		if permissionSetHas(inv.Runtime, downloadAlwaysDenyKey, name) {
			return "", fmt.Errorf("%w: tool %q is always rejected for download", ErrToolPermissionDenied, name)
		}
		if permissionSetHas(inv.Runtime, downloadAlwaysAllowKey, name) {
			return next(ctx, inv)
		}

		reg := getDefaultDownloadRegexps()
		if len(cfg.Patterns) > 0 {
			reg = compileDownloadPatterns(cfg.Patterns)
		}

		matched := matchDownloadPatterns(reg, inv.ArgsJSON)
		if matched == "" {
			return next(ctx, inv)
		}

		title := ResolveToolTitle(inv.Tool.DisplayName, name, inv.ArgsJSON)
		initPayload, _ := json.Marshal(map[string]any{
			"toolName":       name,
			"title":          title,
			"reason":         "download_detected",
			"matchedPattern": matched,
			"args":           inv.ArgsJSON,
		})
		intr, err := inv.Runtime.RaiseInterrupt("tool_permission", initPayload)
		if err != nil {
			return "", err
		}
		perm, ok := intr.(*interrupt.ToolPermissionInterrupt)
		if !ok || perm == nil {
			return "", fmt.Errorf("download permission: unexpected interrupt type %T", intr)
		}

		switch perm.SelectedKind {
		case interrupt.PermissionAllowAlways:
			permissionRemember(inv.Runtime, downloadAlwaysAllowKey, name)
		case interrupt.PermissionRejectAlways:
			permissionRemember(inv.Runtime, downloadAlwaysDenyKey, name)
		}

		if !perm.Allowed {
			return "", fmt.Errorf("%w: user rejected download for tool %q", ErrToolPermissionDenied, name)
		}
		return next(ctx, inv)
	}
}
