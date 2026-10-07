package deps_test

import (
	"os/exec"
	"strings"
	"testing"
)

// The build graph is the module boundary. An unimported package must not appear.
func TestImportGraph(t *testing.T) {
	cases := []struct {
		pkg    string
		direct bool
		absent []string
	}{
		{
			pkg: "github.com/ryanaldo34/tacklr/brain",
			absent: []string{
				"github.com/ryanaldo34/tacklr/vfs",
				"github.com/hanwen/go-fuse",
				"github.com/aws/aws-sdk-go-v2",
			},
		},
		{
			pkg: "github.com/ryanaldo34/tacklr/vfs",
			absent: []string{
				"github.com/ryanaldo34/tacklr/brain",
				"github.com/jackc/pgx",
			},
		},
		{
			pkg:    "github.com/ryanaldo34/tacklr/server",
			direct: true,
			absent: []string{
				"github.com/ryanaldo34/tacklr/server/acp",
				"github.com/ryanaldo34/tacklr/vfs",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.pkg, func(t *testing.T) {
			args := []string{"list", "-deps", tc.pkg}
			if tc.direct {
				args = []string{"list", "-f", "{{join .Imports \"\\n\"}}", tc.pkg}
			}
			out, err := exec.Command("go", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("go list: %v\n%s", err, out)
			}
			deps := string(out)
			for _, bad := range tc.absent {
				if strings.Contains(deps, bad) {
					t.Errorf("dependency %s", bad)
				}
			}
		})
	}
}
