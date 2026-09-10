//go:build ignore
// +build ignore

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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// cmdPkg returns the import path of this module's cmd package.
//
// ELM 2026-09-09. These seven targets were hardcoded to
// github.com/minio/minio/cmd, which 4fd5af8cf left behind when it renamed the
// module. The linker silently ignores a -X whose symbol does not exist, so
// every binary built here reported "DEVELOPMENT.GOGET" with no commit id and a
// copyright year of 0000, and nothing failed to warn about it.
//
// Resolved from the module rather than written out, so it cannot drift again.
// This program already shells out to git, so shelling out to go is in keeping.
func cmdPkg() string {
	out, err := exec.Command("go", "list", "-m").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error resolving module path: ", err)
		os.Exit(1)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" {
		fmt.Fprintln(os.Stderr, "Error resolving module path: empty result from 'go list -m'")
		os.Exit(1)
	}
	return mod + "/cmd."
}

func genLDFlags(version string) string {
	releaseTag, date := releaseTag(version)
	copyrightYear := strconv.Itoa(date.Year())
	pkg := cmdPkg()
	ldflagsStr := "-s -w"
	ldflagsStr += " -X " + pkg + "Version=" + version
	ldflagsStr += " -X " + pkg + "CopyrightYear=" + copyrightYear
	ldflagsStr += " -X " + pkg + "ReleaseTag=" + releaseTag
	ldflagsStr += " -X " + pkg + "CommitID=" + commitID()
	ldflagsStr += " -X " + pkg + "ShortCommitID=" + commitID()[:12]
	ldflagsStr += " -X " + pkg + "GOPATH=" + os.Getenv("GOPATH")
	ldflagsStr += " -X " + pkg + "GOROOT=" + os.Getenv("GOROOT")
	return ldflagsStr
}

// genReleaseTag prints release tag to the console for easy git tagging.
func releaseTag(version string) (string, time.Time) {
	relPrefix := "DEVELOPMENT"
	if prefix := os.Getenv("MINIO_RELEASE"); prefix != "" {
		relPrefix = prefix
	}

	relSuffix := ""
	if hotfix := os.Getenv("MINIO_HOTFIX"); hotfix != "" {
		relSuffix = hotfix
	}

	relTag := strings.Replace(version, " ", "-", -1)
	relTag = strings.Replace(relTag, ":", "-", -1)
	t, err := time.Parse("2006-01-02T15-04-05Z", relTag)
	if err != nil {
		panic(err)
	}
	relTag = strings.Replace(relTag, ",", "", -1)
	relTag = relPrefix + "." + relTag
	if relSuffix != "" {
		relTag += "." + relSuffix
	}

	return relTag, t
}

// commitID returns the abbreviated commit-id hash of the last commit.
func commitID() string {
	// git log --format="%H" -n1
	var (
		commit []byte
		err    error
	)
	cmdName := "git"
	cmdArgs := []string{"log", "--format=%H", "-n1"}
	if commit, err = exec.Command(cmdName, cmdArgs...).Output(); err != nil {
		fmt.Fprintln(os.Stderr, "Error generating git commit-id: ", err)
		os.Exit(1)
	}

	return strings.TrimSpace(string(commit))
}

func commitTime() time.Time {
	// git log --format=%cD -n1
	var (
		commitUnix []byte
		err        error
	)
	cmdName := "git"
	cmdArgs := []string{"log", "--format=%cI", "-n1"}
	if commitUnix, err = exec.Command(cmdName, cmdArgs...).Output(); err != nil {
		fmt.Fprintln(os.Stderr, "Error generating git commit-time: ", err)
		os.Exit(1)
	}

	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(commitUnix)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error generating git commit-time: ", err)
		os.Exit(1)
	}

	return t.UTC()
}

func main() {
	var version string
	if len(os.Args) > 1 {
		version = os.Args[1]
	} else {
		version = commitTime().Format(time.RFC3339)
	}

	fmt.Println(genLDFlags(version))
}
