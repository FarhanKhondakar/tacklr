package brain

import (
	"cmp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	defaultSnippetCap = 240
	snippetLookbehind = 40
)

// promotedParent is a parent candidate after part aggregation.
type promotedParent struct {
	ParentID uuid.UUID
	Score    float64
	Evidence []Evidence
}

// promoteParents groups hits by parent. A hit with no parent_id is itself a parent
// (no evidence). Part hits attach as evidence under parent_id.
// When a parent has evidence, its score is the sum of the kept evidence scores.
// A parent with no parts keeps its own score. A zero evidenceN selects 3.
// A zero snippetCap selects defaultSnippetCap. query places the quote on the match.
func promoteParents(parts []ScoredID, evidenceN int, query string, snippetCap int) []promotedParent {
	evidenceN = cmp.Or(evidenceN, 3)
	snippetCap = cmp.Or(snippetCap, defaultSnippetCap)
	type bucket struct {
		score    float64
		evidence []Evidence
	}
	byParent := make(map[uuid.UUID]bucket, len(parts))
	order := make([]uuid.UUID, 0, len(parts))

	for _, p := range parts {
		pid := p.ID
		isPart := p.ParentID != nil
		if isPart {
			pid = *p.ParentID
		}
		b, ok := byParent[pid]
		if !ok {
			b = bucket{score: p.Score}
			order = append(order, pid)
		} else if p.Score > b.score {
			b.score = p.Score
		}
		if isPart {
			b.evidence = append(b.evidence, Evidence{
				PartID:     p.ID,
				Title:      p.Title,
				Snippet:    snippetAround(p.Content, query, snippetCap),
				Score:      p.Score,
				Position:   p.Position,
				Properties: p.Properties,
			})
		}
		byParent[pid] = b
	}

	out := make([]promotedParent, 0, len(order))
	for _, pid := range order {
		b := byParent[pid]
		slices.SortFunc(b.evidence, func(a, b Evidence) int {
			if c := cmp.Compare(b.Score, a.Score); c != 0 {
				return c
			}
			return cmpUUID(a.PartID, b.PartID)
		})
		if len(b.evidence) > evidenceN {
			b.evidence = b.evidence[:evidenceN]
		}
		if len(b.evidence) > 0 {
			b.score = 0
			for _, ev := range b.evidence {
				b.score += ev.Score
			}
		}
		out = append(out, promotedParent{
			ParentID: pid,
			Score:    b.score,
			Evidence: b.evidence,
		})
	}
	slices.SortFunc(out, func(a, b promotedParent) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return cmpUUID(a.ParentID, b.ParentID)
	})
	return out
}

// snippetAround returns at most maxRunes of s, starting snippetLookbehind runes
// before the earliest query word. An empty query, or a query with no word in s,
// keeps the prefix. A window that is not the full text gains an ellipsis on the cut side.
func snippetAround(s, query string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if s == "" || maxRunes <= 0 {
		return s
	}
	folded := strings.ToLower(s)
	idx := -1
	for _, tok := range strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}) {
		if i := strings.Index(folded, tok); i >= 0 && (idx < 0 || i < idx) {
			idx = i
		}
	}
	if idx < 0 {
		cut := capRunes(s, maxRunes)
		if cut == s {
			return s
		}
		return cut + "…"
	}
	start := idx
	for n := snippetLookbehind; n > 0 && start > 0; n-- {
		start--
		for start > 0 && !utf8.RuneStart(s[start]) {
			start--
		}
	}
	cut := capRunes(s[start:], maxRunes)
	out := cut
	if start > 0 {
		out = "…" + out
	}
	if start+len(cut) < len(s) {
		out += "…"
	}
	return out
}
