#!/bin/bash

infra-start() {
    local default_buckets="mobile-ingest mobile-verify mobile-reward iot-ingest iot-verify iot-reward poclora-ingest poclora-verifier"

    if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]];
       then echo "required creds unset"; return 1
    else
        MINIO_ROOT_USER=${MINIO_ROOT_USER:-novaadmin} \
            MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-novaadmin} \
            MINIO_BUCKETS=${MINIO_BUCKETS:-"$default_buckets"} \
            docker-compose up -d
    fi
}

infra-stop() {
    docker-compose down
    if [[ ${1} == "drop" ]];
        then
            volumes=($(docker volume ls | grep -e bucket-data -e db-data | awk '{print $2}'))
            for vol in "${volumes[@]}"; do docker volume rm $vol; done
    fi
}
"$@"
