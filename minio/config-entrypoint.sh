#!/bin/sh

sleep 3

/usr/bin/mc alias set localminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

for bucket in ${MINIO_BUCKETS}; do
    if ! /usr/bin/mc ls localminio/${bucket} > /dev/null 2>&1 ; then
        /usr/bin/mc mb localminio/${bucket}
    fi
done

/usr/bin/mc admin policy add localminio fullaccess /bucket-policy.json

/usr/bin/mc admin user add localminio ${TEST_USER_ID} ${TEST_USER_KEY}
/usr/bin/mc admin policy set localminio fullaccess user=${TEST_USER_ID}
