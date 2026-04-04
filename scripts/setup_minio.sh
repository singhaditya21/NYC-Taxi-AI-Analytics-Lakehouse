#!/bin/bash
alias mc='docker run -it --rm -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=password bitnami/minio mc'
mc alias set myminio http://minio:9000 admin password
mc mb myminio/lakehouse
echo "Bucket configured."\n