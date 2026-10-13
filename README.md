# stanford-rc/minio

**This is a modified version of MinIO.** It is not MinIO, Inc. software, it is
not supported by MinIO, Inc., and it has not been reviewed or endorsed by them.

Modified by The Board of Trustees of the Leland Stanford Junior University,
Stanford Research Computing, between **2026-02-25 and 2026-10-12**.

Forked from `github.com/minio/minio` at upstream commit `be7800c81`, dated
2026-01-05. The Go module path was changed to `github.com/stanford-rc/minio` so
that this fork and upstream cannot be confused in a build.

This program is free software, released under the **GNU Affero General Public
License version 3**, the same license as the upstream work. See `LICENSE` for the
full text and `NOTICE` for upstream attribution. Copyright in the unmodified
portions remains with MinIO, Inc. and the upstream contributors.

MinIO(R) is a registered trademark of MinIO, Inc. It is used here only to
identify the software this fork is derived from.

## Why this fork exists

Stanford Research Computing runs this build as the object store behind Elm, its
S3 service. The fork exists because the changes below cannot be expressed through
`mc admin config`, and because upstream MinIO is no longer developed in the open,
so there is nowhere to send them.

## What was changed

Three behavioural divergences, each controlled by an environment variable so it
can be turned off per process without a rebuild:

| divergence | default | override |
|---|---|---|
| minimum multipart part size | 5 GiB, upstream is 5 MiB | `MINIO_MIN_PART_SIZE` |
| write-set shortfall reporting and repair queueing | on | `MINIO_MULTIPART_WRITESET=off` |
| dangling-object deletion during heal | off, upstream always deletes | `MINIO_DANGLING_DELETE=on` |

The write-set work is the substantive one. MinIO commits a multipart upload in
two phases. Each part's shards are written to a staging area as the part is
uploaded, and `CompleteMultipartUpload` then renames the staged data directory
into place on each drive. Each phase succeeds once a write quorum of drives
accepts it, and upstream never compares which drives took which part. A part
admitted on exactly as many drives as it has data blocks can therefore be
committed on a write quorum that leaves out one of the drives it depends on.
That part cannot be read back, and upstream returns `200 OK` for the object.

The fork fails that commit. When the drives that committed leave any part below
its data blocks, `CompleteMultipartUpload` returns an error, so the client knows,
and the surviving staging shards are kept for recovery. This check runs whatever
`MINIO_MULTIPART_WRITESET` is set to.

A shortfall that leaves the object readable is accepted rather than refused,
because refusing would make clients re-send terabytes during the bursts in which
these faults arrive. The fork logs it and queues the repair:

- a multipart part that landed on fewer drives than it was written to is logged
  as a `write-set shortfall`, and the commit queues a heal for the object
- a single-part PUT whose shards or `xl.meta` missed a drive is logged the same
  way, one line for every affected PUT, and MinIO's own repair queue heals it
  after the commit
- a single-part PUT written around a drive that was already unavailable logs a
  line that names no object, so each distinct combination of drives and parity
  outcome is logged once per node per hour rather than once per object

`MINIO_MULTIPART_WRITESET=off` turns off this logging, single-part and
multipart alike, and the multipart heal queueing. It leaves the commit failure
in place. A single-part PUT also drops any drive whose shard write failed
before the commit, whatever the switch is set to, because that drive's commit
would fail anyway.

Alongside those:

- btrfs added to the filesystem type map, so btrfs-backed drives are recognised
- the build derives its version string and module path from the repository
  rather than from hard-coded upstream values
- a ring buffer fix, and corrections to static strings that were wrong in the
  initial fork
- build and test targets made to work offline, with pinned lint tooling

`git log` is the authoritative list. This section is a summary and will drift.

## Source availability

The complete corresponding source for this modified version is this repository.
If you interact with a Stanford Research Computing service that runs this build
and you want the source, it is here.

## Upstream documentation

This README replaces upstream's. For how to run, configure and operate MinIO,
use upstream's documentation and read it against the divergences above:

- https://github.com/minio/minio
- https://min.io/docs/

## Do not downgrade

Newer MinIO releases can write on-disk formats that older releases cannot read.
Consult the
[MinIO Downgrade Warning](https://github.com/stanford-rc/elm/wiki/MinIO-Downgrade-Warning)
before moving a deployment backwards.

## Building

Tags on this fork are `STANFORD.<timestamp>`. The build derives the binary's
version string from the tag name, so the `T##-##-##Z` shape is load-bearing. Note
that the binary reports `RELEASE.<timestamp>` rather than `STANFORD.<timestamp>`,
because the build sets `MINIO_RELEASE=RELEASE`; match a running binary to a tag
by commit id.

```bash
make
```
