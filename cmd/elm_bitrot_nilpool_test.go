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
	"context"
	"io"
	"testing"

	"github.com/stanford-rc/minio/internal/bpool"
)

// Elm, 2026-09-09. The case nothing covered: a streaming bitrot writer built
// while globalBytePoolCap holds no pool.
//
// This is the condition upstream d4b391de1 (#19605) introduced and left
// unguarded. It is normally invisible because an earlier test populates the pool
// as a side effect of building an ObjectLayer, and nothing resets it, so the
// whole package inherits a usable pool and the fault never fires. TestMain now
// populates it deliberately, which means the unguarded path would be untested
// from here on -- hence this test clears the pool explicitly.
//
// Without the guard in bitrotWriterBuffer this panics with integer divide by
// zero inside internal/ringbuffer, on a goroutine detached from this test, which
// takes down the whole package binary rather than failing one test.
func TestStreamingBitrotWriterWithNoBytePool(t *testing.T) {
	// Clear the pool for the duration, then restore. Restoring matters more than
	// usual: leaving it nil would re-arm the original fault for every test that
	// ran afterwards, and the failure would surface nowhere near here.
	saved := globalBytePoolCap.Load()
	globalBytePoolCap.Store(nil)
	t.Cleanup(func() { globalBytePoolCap.Store(saved) })

	if got := globalBytePoolCap.Load().Get(); cap(got) != 0 {
		t.Fatalf("precondition: expected a nil pool to yield a zero-cap slice, got cap %d", cap(got))
	}

	// The guard must not hand back something unusable.
	if buf := bitrotWriterBuffer(); cap(buf) == 0 {
		t.Fatal("bitrotWriterBuffer returned a zero-capacity buffer; the ring buffer would divide by zero")
	}

	tmpDir := t.TempDir()
	disk, err := newLocalXLStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	const volume, filePath = "testvol", "testfile"
	if err := disk.MakeVol(context.Background(), volume); err != nil {
		t.Fatal(err)
	}

	// Same shape as testBitrotReaderWriterAlgo: 35 bytes in 10-byte shards,
	// through the streaming path that HighwayHash256S selects.
	writer := newBitrotWriter(disk, "", volume, filePath, 35, HighwayHash256S, 10)
	for _, chunk := range [][]byte{
		[]byte("aaaaaaaaaa"), []byte("aaaaaaaaaa"),
		[]byte("aaaaaaaaaa"), []byte("aaaaa"),
	} {
		if _, err := writer.Write(chunk); err != nil {
			t.Fatalf("write with no byte pool: %v", err)
		}
	}
	if bw, ok := writer.(io.Closer); ok {
		if err := bw.Close(); err != nil {
			t.Fatalf("close with no byte pool: %v", err)
		}
	}

	// And it must have actually written, not just failed quietly.
	reader := newBitrotReader(disk, nil, volume, filePath, 35, HighwayHash256S, bitrotWriterSum(writer), 10)
	got := make([]byte, 10)
	if _, err := reader.ReadAt(got, 0); err != nil {
		t.Fatalf("read back with no byte pool: %v", err)
	}
	if string(got) != "aaaaaaaaaa" {
		t.Fatalf("read back %q, want %q", got, "aaaaaaaaaa")
	}
}

// The pool must be restored by the test above, since a leaked nil would re-arm
// the original panic for every later test in the package.
func TestBytePoolIsInitializedForTests(t *testing.T) {
	pool := globalBytePoolCap.Load()
	if pool == nil {
		t.Fatal("globalBytePoolCap is nil; TestMain must populate it, see the comment there")
	}
	if _, ok := any(pool).(*bpool.BytePoolCap); !ok {
		t.Fatalf("unexpected pool type %T", pool)
	}
	if cap(pool.Get()) == 0 {
		t.Fatal("the test byte pool hands out zero-capacity buffers")
	}
}
