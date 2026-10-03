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
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

// xlMetaWithMetaV serializes a two-version xl.meta and rewrites its meta
// version byte to metaV, recomputing the payload CRC so that Load gets as far
// as the version check instead of failing on the checksum.
func xlMetaWithMetaV(t *testing.T, metaV uint8) (buf []byte, versionIDs []string) {
	t.Helper()

	xl := xlMetaV2{}
	fi := FileInfo{
		Volume:    "volume",
		Name:      "object-name",
		VersionID: mustGetUUID(),
		IsLatest:  true,
		DataDir:   mustGetUUID(),
		ModTime:   time.Now(),
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   3,
			ParityBlocks: 1,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4},
			Checksums: []ChecksumInfo{{
				PartNumber: 1,
				Algorithm:  HighwayHash256S,
			}},
		},
		Data:        []byte("some object data"),
		NumVersions: 1,
	}
	if err := xl.AddVersion(fi); err != nil {
		t.Fatal(err)
	}
	versionIDs = append(versionIDs, fi.VersionID)

	fi.VersionID = mustGetUUID()
	fi.DataDir = mustGetUUID()
	fi.ModTime = fi.ModTime.Add(time.Second)
	if err := xl.AddVersion(fi); err != nil {
		t.Fatal(err)
	}
	versionIDs = append(versionIDs, fi.VersionID)

	buf, err := xl.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Layout from AppendTo: xlHeader, xlVersionCurrent, a bin32 header holding
	// the payload length, then the payload, which opens with the header version
	// and the meta version as one-byte msgp fixints, then a 5 byte CRC.
	dataOffset := len(xlHeader) + len(xlVersionCurrent) + 5
	payloadLen := int(binary.BigEndian.Uint32(buf[dataOffset-4 : dataOffset]))
	if buf[dataOffset] != xlHeaderVersion || buf[dataOffset+1] != xlMetaVersion {
		t.Fatalf("unexpected layout: header version %d, meta version %d", buf[dataOffset], buf[dataOffset+1])
	}
	buf[dataOffset+1] = metaV

	crcAt := dataOffset + payloadLen
	if buf[crcAt] != 0xce {
		t.Fatalf("unexpected CRC marker 0x%x at %d", buf[crcAt], crcAt)
	}
	binary.BigEndian.PutUint32(buf[crcAt+1:crcAt+5], uint32(xxhash.Sum64(buf[dataOffset:crcAt])))

	return buf, versionIDs
}

// TestXLMetaReadsMetaVersion3 checks that this release reads xl.meta written
// at meta version 3, which has the same layout as version 2, so that it can
// run against drives a newer release has written.
func TestXLMetaReadsMetaVersion3(t *testing.T) {
	buf, versionIDs := xlMetaWithMetaV(t, 3)

	var xl xlMetaV2
	if err := xl.Load(buf); err != nil {
		t.Fatalf("Load of meta version 3: %v", err)
	}
	if xl.metaV != 3 {
		t.Fatalf("metaV = %d, want 3", xl.metaV)
	}

	// ToFileInfo decodes each version through xlMetaV2Version.unmarshalV,
	// which carries its own meta version check.
	for _, id := range versionIDs {
		fi, err := xl.ToFileInfo("volume", "object-name", id, false, true)
		if err != nil {
			t.Fatalf("ToFileInfo(%s): %v", id, err)
		}
		if fi.VersionID != id {
			t.Fatalf("ToFileInfo returned version %s, want %s", fi.VersionID, id)
		}
		if !bytes.Equal(xl.data.find(id), []byte("some object data")) {
			t.Fatalf("inline data for %s not readable", id)
		}
	}

	// Writes stay at xlMetaVersion, so a newer release still runs its
	// version 3 repair check on anything this release rewrites.
	out, err := xl.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	dataOffset := len(xlHeader) + len(xlVersionCurrent) + 5
	if out[dataOffset+1] != xlMetaVersion {
		t.Fatalf("rewrite meta version = %d, want %d", out[dataOffset+1], xlMetaVersion)
	}
}

// TestXLMetaRejectsMetaVersionAboveReadMax checks that the read gate still
// refuses meta versions newer than xlMetaVersionReadMax.
func TestXLMetaRejectsMetaVersionAboveReadMax(t *testing.T) {
	buf, _ := xlMetaWithMetaV(t, xlMetaVersionReadMax+1)

	var xl xlMetaV2
	err := xl.Load(buf)
	if err == nil {
		t.Fatalf("Load of meta version %d succeeded, want an error", xlMetaVersionReadMax+1)
	}
	if !strings.Contains(err.Error(), "Unknown xl meta version") {
		t.Fatalf("Load of meta version %d: %v, want an unknown meta version error", xlMetaVersionReadMax+1, err)
	}

	var ver xlMetaV2Version
	if _, err := ver.unmarshalV(xlMetaVersionReadMax+1, nil); err == nil {
		t.Fatalf("unmarshalV(%d) succeeded, want an error", xlMetaVersionReadMax+1)
	}
}
