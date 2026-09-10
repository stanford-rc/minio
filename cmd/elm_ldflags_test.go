// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
)

// TestGenLDFlagsTargetsThisModule guards the -X targets that buildscripts/
// gen-ldflags.go emits.
//
// ELM 2026-09-09. All seven were hardcoded to github.com/minio/minio/cmd, which
// 4fd5af8cf left behind when it renamed the module. The linker silently ignores
// a -X naming a symbol that does not exist, so every binary built from this fork
// reported "DEVELOPMENT.GOGET" with no commit id and a copyright year of 0000.
// Nothing failed, and nothing warned.
//
// This is the check that would have caught it: the target package path has to be
// this module's cmd package, whatever this module happens to be called.
func TestGenLDFlagsTargetsThisModule(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Main.Path == "" {
		t.Skip("build info unavailable, cannot determine the module path")
	}
	wantPkg := bi.Main.Path + "/cmd."

	// gen-ldflags.go carries a //go:build ignore tag, so it is only reachable
	// through "go run". It resolves the module with "go list -m" and the commit
	// with "git log", both of which work from any directory inside the repo.
	out, err := exec.Command("go", "run", "../buildscripts/gen-ldflags.go").CombinedOutput()
	if err != nil {
		t.Fatalf("go run ../buildscripts/gen-ldflags.go: %v\n%s", err, out)
	}
	got := strings.TrimSpace(string(out))

	// Every variable cmd/build-constants.go expects to be filled in at link time.
	for _, name := range []string{
		"Version", "CopyrightYear", "ReleaseTag",
		"CommitID", "ShortCommitID", "GOPATH", "GOROOT",
	} {
		want := "-X " + wantPkg + name + "="
		if !strings.Contains(got, want) {
			t.Errorf("ldflags missing %q\ngot: %s", want, got)
		}
	}

	if strings.Contains(got, "github.com/minio/minio/cmd.") && wantPkg != "github.com/minio/minio/cmd." {
		t.Errorf("ldflags still target the pre-rename upstream package path; "+
			"gen-ldflags.go should resolve the module rather than hardcode it\ngot: %s", got)
	}
}

// TestBuildConstantsAreLinkerTargets keeps the variable set in
// cmd/build-constants.go and the -X list in gen-ldflags.go from drifting apart.
// A variable added here without a matching -X stays empty in every release
// build, which is the same silent failure as the wrong package path.
func TestBuildConstantsAreLinkerTargets(t *testing.T) {
	out, err := exec.Command("go", "run", "../buildscripts/gen-ldflags.go").CombinedOutput()
	if err != nil {
		t.Fatalf("go run ../buildscripts/gen-ldflags.go: %v\n%s", err, out)
	}
	got := string(out)

	// Kept deliberately explicit rather than reflected, so adding a build
	// constant means consciously deciding whether it needs a linker target.
	for _, name := range []string{
		"Version", "CopyrightYear", "ReleaseTag",
		"CommitID", "ShortCommitID", "GOPATH", "GOROOT",
	} {
		if !strings.Contains(got, "."+name+"=") {
			t.Errorf("build constant %q has no -X target in gen-ldflags.go", name)
		}
	}
}
