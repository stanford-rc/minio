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
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestGetHandlerName guards the module path in elmCmdPkgPrefix.
//
// ELM 2026-09-09. getHandlerName strips a hardcoded package prefix from a
// reflected symbol name. When 4fd5af8cf renamed the module it rewrote the import
// statements but not that string literal, so the prefix stopped matching and the
// function began returning the full package path. Nothing caught it, because
// nothing tested this function.
//
// That is not cosmetic. The return value reaches ReqInfo.API through newContext
// in cmd/admin-router.go, which is the audit log's msg.api.name, and it labels
// the API metrics through collectAPIStats in cmd/api-router.go. A build carrying
// the defect emits "github.com/stanford-rc/minio/cmd.objectAPIHandlers.PutObjectPart"
// where every downstream query expects "PutObjectPart".
func TestGetHandlerName(t *testing.T) {
	objAPI := objectAPIHandlers{}
	admAPI := adminAPIHandlers{}

	testCases := []struct {
		name     string
		f        http.HandlerFunc
		cmdType  string
		expected string
	}{
		{"object PutObjectPart", objAPI.PutObjectPartHandler, "objectAPIHandlers", "PutObjectPart"},
		{"object CompleteMultipartUpload", objAPI.CompleteMultipartUploadHandler, "objectAPIHandlers", "CompleteMultipartUpload"},
		{"object GetObject", objAPI.GetObjectHandler, "objectAPIHandlers", "GetObject"},
		{"admin Heal", admAPI.HealHandler, "adminAPIHandlers", "Heal"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getHandlerName(tc.f, tc.cmdType)
			if got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
			// The specific regression: a stale prefix leaves the module path in
			// place, and the assertion above would catch it, but this says why.
			if strings.Contains(got, "/") {
				t.Errorf("handler name still carries a package path: %q. "+
					"elmCmdPkgPrefix in cmd/api-utils.go must match this module.", got)
			}
		})
	}
}

// TestElmCmdPkgPrefixMatchesThisModule fails if the constant drifts from the
// actual package path, independently of any particular handler.
func TestElmCmdPkgPrefixMatchesThisModule(t *testing.T) {
	// getSource(1) reports this function, so its untrimmed name is this package's
	// path plus the function name. If the prefix is right, nothing is left over.
	full := getSource(1)
	if strings.Contains(full, "/") {
		t.Errorf("getSource leaked a package path: %q. elmCmdPkgPrefix is %q, "+
			"which no longer matches this module.", full, elmCmdPkgPrefix)
	}
	if !strings.Contains(full, "TestElmCmdPkgPrefixMatchesThisModule") {
		t.Errorf("expected the caller's function name in %q", full)
	}
}

func TestS3EncodeName(t *testing.T) {
	testCases := []struct {
		inputText, encodingType, expectedOutput string
	}{
		{"a b", "", "a b"},
		{"a b", "url", "a+b"},
		{"p- ", "url", "p-+"},
		{"p-%", "url", "p-%25"},
		{"p/", "url", "p/"},
		{"p/", "url", "p/"},
		{"~user", "url", "%7Euser"},
		{"*user", "url", "*user"},
		{"user+password", "url", "user%2Bpassword"},
		{"_user", "url", "_user"},
		{"firstname.lastname", "url", "firstname.lastname"},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			outputText := s3EncodeName(testCase.inputText, testCase.encodingType)
			if testCase.expectedOutput != outputText {
				t.Errorf("Expected `%s`, got `%s`", testCase.expectedOutput, outputText)
			}
		})
	}
}
