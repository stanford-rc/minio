module github.com/stanford-rc/minio/docs/debugging/reorder-disks

go 1.21

toolchain go1.24.8

require github.com/minio/pkg/v3 v3.0.1

replace github.com/minio/madmin-go/v3 => github.com/stanford-rc/madmin-go/v3 v3.0.51

replace github.com/minio/md5-simd => github.com/stanford-rc/minio-md5-simd v1.1.2

replace github.com/minio/minio-go/v7 => github.com/stanford-rc/minio-go/v7 v7.0.70

replace github.com/minio/mux => github.com/stanford-rc/minio-mux v1.8.2

replace github.com/minio/pkg/v3 => github.com/stanford-rc/minio-pkg/v3 v3.0.1
