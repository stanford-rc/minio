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

package logger

import (
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

// TestInitTrimsThisModulePath guards the module path Init appends to trimStrings.
//
// ELM 2026-09-09. It was hardcoded to github.com/minio/minio, which 4fd5af8cf
// left behind when it renamed the module. trimTrace then stopped trimming
// anything, and every logged source path carried the full package path. Nothing
// caught it because this package had no tests at all.
func TestInitTrimsThisModulePath(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Main.Path == "" {
		t.Skip("build info unavailable, cannot determine the module path")
	}
	want := filepath.FromSlash(bi.Main.Path) + string(filepath.Separator)

	saved := trimStrings
	t.Cleanup(func() { trimStrings = saved })

	Init("", "")

	found := false
	for _, s := range trimStrings {
		if s == want {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Init did not add %q to trimStrings.\ngot: %q", want, trimStrings)
	}

	// The property that actually matters: a source path under this module gets
	// trimmed down to something readable.
	full := bi.Main.Path + "/cmd/erasure-multipart.go"
	if got := trimTrace(full); got != "cmd/erasure-multipart.go" {
		t.Errorf("trimTrace(%q) = %q, want %q", full, got, "cmd/erasure-multipart.go")
	}
}

// TestInitHasNoStaleUpstreamPath fails if the pre-rename literal comes back as a
// trim string, which would mean the derivation was replaced by a hardcoded path
// again.
func TestInitHasNoStaleUpstreamPath(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Main.Path == "github.com/minio/minio" {
		t.Skip("running as upstream minio, where that path is not stale")
	}

	saved := trimStrings
	t.Cleanup(func() { trimStrings = saved })

	Init("", "")

	stale := filepath.FromSlash("github.com/minio/minio") + string(filepath.Separator)
	for _, s := range trimStrings {
		if strings.HasSuffix(s, stale) {
			t.Errorf("trimStrings still carries the pre-rename upstream path %q; "+
				"Init should derive the module path from debug.ReadBuildInfo", s)
		}
	}
}
