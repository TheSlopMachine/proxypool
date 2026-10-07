//go:build ignore

// Publish cuts and pushes the next release tag for the proxypool library.
// Tags are the source of truth: vX.Y.Z, short vX.Y when Fix is 0.
// Consumers (llm-router) pin the tag in go.mod; no CI build runs on tag.
//
// Usage:
//
//	go run publish.go -t fix|minor|major
//	go run publish.go -v X.Y[.Z] [--notes ...]
//	go run publish.go -h
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type triple struct {
	major, minor, fix int
}

func parseTriple(s string) (triple, error) {
	t := strings.TrimSpace(s)
	t = strings.TrimPrefix(t, "v")
	t = strings.TrimPrefix(t, "V")
	parts := strings.Split(t, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return triple{}, fmt.Errorf("expected X.Y[.Z], got %q", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return triple{}, fmt.Errorf("invalid empty component in %q", s)
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return triple{}, fmt.Errorf("invalid numeric component %q in %q", p, s)
		}
		nums[i] = n
	}
	return triple{major: nums[0], minor: nums[1], fix: nums[2]}, nil
}

func (t triple) tag() string {
	if t.fix == 0 {
		return fmt.Sprintf("v%d.%d", t.major, t.minor)
	}
	return fmt.Sprintf("v%d.%d.%d", t.major, t.minor, t.fix)
}

func (t triple) less(o triple) bool {
	if t.major != o.major {
		return t.major < o.major
	}
	if t.minor != o.minor {
		return t.minor < o.minor
	}
	return t.fix < o.fix
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[FAIL] "+format+"\n", args...)
	os.Exit(1)
}

func git(args ...string) string {
	cmd := exec.Command("git", args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		failf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

func gitOK(args ...string) (string, bool) {
	cmd := exec.Command("git", args...)
	cmd.Stderr = nil
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

const usage = `Usage:
  go run publish.go -t fix|minor|major
  go run publish.go -v X.Y[.Z]
  go run publish.go -h

Tags are the source of truth (vX.Y.Z, short vX.Y when fix is 0).
The tree must be clean with HEAD synced to origin/main.
Creates and pushes the annotated tag; no build runs.
`

func main() {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	help := fs.Bool("h", false, "print help")
	helpLong := fs.Bool("help", false, "print help")
	bump := fs.String("t", "", "bump type: fix|minor|major")
	bumpLong := fs.String("type", "", "bump type: fix|minor|major")
	forced := fs.String("v", "", "force version X.Y[.Z]")
	forcedLong := fs.String("version", "", "force version X.Y[.Z]")
	notes := fs.String("notes", "", "extra tag annotation")
	if err := fs.Parse(os.Args[1:]); err != nil {
		failf("%v", err)
	}
	if *help || *helpLong {
		fmt.Print(usage)
		return
	}
	typ := strings.ToLower(strings.TrimSpace(*bump + *bumpLong))
	if *bump != "" && *bumpLong != "" {
		failf("-t and --type are the same flag: set one")
	}
	ver := strings.TrimSpace(*forced + *forcedLong)
	if *forced != "" && *forcedLong != "" {
		failf("-v and --version are the same flag: set one")
	}
	if typ != "" && ver != "" {
		failf("-t and -v are mutually exclusive: set one")
	}
	if typ == "" && ver == "" {
		failf("set -t fix|minor|major or -v X.Y[.Z]")
	}

	if _, ok := gitOK("rev-parse", "--git-dir"); !ok {
		failf("not a git checkout: publish requires git tags")
	}
	if status := git("status", "--porcelain"); status != "" {
		failf("tree is dirty: commit or stash first:\n%s", status)
	}
	if branch := git("rev-parse", "--abbrev-ref", "HEAD"); branch != "main" {
		failf("publish only from main, current branch is %q", branch)
	}
	if _, ok := gitOK("fetch", "origin"); !ok {
		failf("git fetch origin failed")
	}
	head := git("rev-parse", "HEAD")
	upstream, ok := gitOK("rev-parse", "origin/main")
	if !ok {
		failf("origin/main is unreachable: push main first")
	}
	if head != upstream {
		failf("HEAD (%s) differs from origin/main (%s): push or pull first", head, upstream)
	}

	latestRaw, ok := gitOK("describe", "--tags", "--abbrev=0", "--match", "v*")
	if !ok || latestRaw == "" {
		if ver == "" {
			failf("no tags found: bootstrap with go run publish.go -v X.Y.Z")
		}
		latestRaw = ""
	}
	var latest triple
	if latestRaw != "" {
		var err error
		latest, err = parseTriple(latestRaw)
		if err != nil {
			failf("latest tag %q is not a version: %v", latestRaw, err)
		}
		if tagCommit, ok := gitOK("rev-list", "-n", "1", latestRaw); ok && tagCommit == head {
			failf("nothing changed since %s: HEAD already tagged", latestRaw)
		}
	}

	var target triple
	switch {
	case ver != "":
		var err error
		target, err = parseTriple(ver)
		if err != nil {
			failf("invalid -v %q: %v", ver, err)
		}
		if _, exists := gitOK("rev-parse", "-q", "--verify", "refs/tags/"+target.tag()); exists {
			failf("tag %s already exists", target.tag())
		}
		if latestRaw != "" && target.less(latest) {
			fmt.Printf("[WARN] target %s is older than latest %s\n", target.tag(), latestRaw)
		}
	case typ == "fix":
		target = triple{major: latest.major, minor: latest.minor, fix: latest.fix + 1}
	case typ == "minor":
		target = triple{major: latest.major, minor: latest.minor + 1}
	case typ == "major":
		target = triple{major: latest.major + 1}
	default:
		failf("invalid -t %q: want fix|minor|major", typ)
	}
	if latestRaw != "" {
		if _, exists := gitOK("rev-parse", "-q", "--verify", "refs/tags/"+target.tag()); exists {
			failf("tag %s already exists", target.tag())
		}
	}

	tagName := target.tag()
	args := []string{"tag", "-a", tagName, "-m", "Release " + tagName}
	if strings.TrimSpace(*notes) != "" {
		args = append(args, "-m", *notes)
	}
	if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
		failf("git tag %s: %v\n%s", tagName, err, string(out))
	}
	if out, err := exec.Command("git", "push", "origin", tagName).CombinedOutput(); err != nil {
		failf("git push origin %s: %v\n%s", tagName, err, string(out))
	}
	fmt.Printf("\n[OK] Published %s (from %s, commit %s)\n", tagName, latestRaw, head)
}
