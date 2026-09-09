// Copyright (c) 2015-2024 MinIO, Inc.
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
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
)

// Elm, 2026-09-08. Support for the globalMinPartSize divergence, which
// moved here from elm-minio's build-time AST rewrite. See cmd/utils.go.

// withElmMinPartSize RAISES the multipart floor to the Elm production default
// for the duration of one test, and restores it afterwards.
//
// Note the direction. TestMain lowers globalMinPartSize to the S3 minimum for
// the whole package, because the production 5 GiB floor cannot be satisfied with
// real data in a unit test and would break every upstream test that completes a
// small multipart upload. So a test that wants to assert the PRODUCTION floor
// has to opt back up, which is what this does.
//
// An earlier revision was the inverse, withS3MinPartSize, lowering the floor per
// test. That was the wrong shape: it made every upstream small-multipart test a
// site that had to be found and patched, against a denominator nobody knew.
//
// Do NOT call this from a test that runs in parallel with another test in this
// package: globalMinPartSize is process-wide state.
func withElmMinPartSize(t *testing.T) {
	t.Helper()
	saved := globalMinPartSize
	globalMinPartSize = elmDefaultMinPartSize
	t.Cleanup(func() { globalMinPartSize = saved })
}

// TestMinPartSizeIsElmFiveGiB pins the production default.
//
// This is the test that could not exist while the value was applied by a
// build-time source rewrite, and it is the main reason the value moved into this
// repository. It fails if someone restores the upstream default, and it fails if
// a merge drops the divergence.
func TestMinPartSizeIsElmFiveGiB(t *testing.T) {
	const want = 5 * humanize.GiByte
	if elmDefaultMinPartSize != want {
		t.Fatalf("elmDefaultMinPartSize = %d, want %d (5 GiB). Elm archives "+
			"every part to tape as a separate file, so the S3 default of 5 MiB lets "+
			"one object consume thousands of inodes. See the comment on the "+
			"declaration.", int64(elmDefaultMinPartSize), int64(want))
	}
	// Asserted through parseMinPartSize rather than globalMinPartSize, because
	// TestMain deliberately lowers the var for the whole package. This is the
	// path a real server takes: serverHandleEnvVars calls parseMinPartSize with
	// the env value, and an unset env must yield the Elm default.
	got, err := parseMinPartSize("")
	if err != nil {
		t.Fatalf("parseMinPartSize(\"\") returned an error: %v", err)
	}
	if got != want {
		t.Fatalf("parseMinPartSize(\"\") = %d, want the Elm default %d", got, int64(want))
	}
	if elmDefaultMinPartSize <= s3MinPartSize {
		t.Fatalf("the Elm default (%d) must exceed the S3 minimum (%d)",
			int64(elmDefaultMinPartSize), int64(s3MinPartSize))
	}
}

// TestParseMinPartSize covers the override rules. The override exists so the
// muse acceptance lab can drive multipart scenarios without writing 5 GiB per
// part; it must not be usable to defeat the tape protection outright.
func TestParseMinPartSize(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		want    int64
		wantErr string
	}{
		{name: "unset gives the production default", raw: "", want: elmDefaultMinPartSize},
		{name: "whitespace is treated as unset", raw: "   ", want: elmDefaultMinPartSize},
		{name: "human GiB", raw: "5GiB", want: 5 * humanize.GiByte},
		{name: "human MiB above the floor", raw: "6MiB", want: 6 * humanize.MiByte},
		{name: "plain byte count", raw: "6291456", want: 6 * humanize.MiByte},
		{name: "surrounding whitespace is tolerated", raw: " 6MiB ", want: 6 * humanize.MiByte},
		{name: "exactly the S3 minimum is allowed", raw: "5MiB", want: s3MinPartSize},

		{name: "one byte under the S3 minimum is refused", raw: "5242879", wantErr: "below the S3 minimum"},
		{name: "a tiny value is refused, not clamped", raw: "1KiB", wantErr: "below the S3 minimum"},
		{name: "zero is refused", raw: "0", wantErr: "below the S3 minimum"},
		{name: "the maximum object size is refused", raw: "5TiB", wantErr: "maximum object size"},
		{name: "garbage is refused", raw: "banana", wantErr: "is not a size"},
		{name: "a negative value is refused", raw: "-1", wantErr: "is not a size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMinPartSize(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parseMinPartSize(%q) = %d, want an error containing %q",
						tc.raw, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseMinPartSize(%q) error = %q, want it to contain %q",
						tc.raw, err, tc.wantErr)
				}
				if got != 0 {
					t.Errorf("a refused value must return 0, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMinPartSize(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("parseMinPartSize(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

// TestParseMinPartSizeNamesTheVariable checks the error text carries the env var
// name, because the operator reading a refused startup needs to know which knob
// to fix.
func TestParseMinPartSizeNamesTheVariable(t *testing.T) {
	_, err := parseMinPartSize("1KiB")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), EnvMinPartSize) {
		t.Errorf("error %q does not name %s", err, EnvMinPartSize)
	}
}

// TestMinAllowedPartSizeUsesTheElmFloor checks the enforcement point rather
// than the constant, because a divergence that the predicate does not read is
// not a divergence.
func TestMinAllowedPartSizeUsesTheElmFloor(t *testing.T) {
	withElmMinPartSize(t)

	if isMinAllowedPartSize(s3MinPartSize) {
		t.Error("a 5 MiB part is accepted; the S3 floor is still in force")
	}
	if isMinAllowedPartSize(5*humanize.GiByte - 1) {
		t.Error("a part one byte under 5 GiB is accepted")
	}
	if !isMinAllowedPartSize(5 * humanize.GiByte) {
		t.Error("a part of exactly 5 GiB is refused; the floor is inclusive")
	}
}

// TestWithElmMinPartSizeRestores guards the helper itself. A leaked override
// would silently RAISE the floor for every test that ran afterwards, breaking
// unrelated small-multipart tests with a failure that points nowhere near here.
func TestWithElmMinPartSizeRestores(t *testing.T) {
	before := globalMinPartSize
	t.Run("inner", func(t *testing.T) {
		withElmMinPartSize(t)
		if globalMinPartSize != elmDefaultMinPartSize {
			t.Fatalf("override did not take: %d", globalMinPartSize)
		}
	})
	if globalMinPartSize != before {
		t.Fatalf("override leaked: globalMinPartSize = %d, want %d",
			globalMinPartSize, before)
	}
}
