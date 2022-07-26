#!/bin/bash

minio-start() {
    if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]];
       then echo "required creds unset"; return 1
    else
        MINIO_ROOT_USER=${MINIO_ROOT_USER:-novaadmin} \
            MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-novaadmin} \
            MINIO_BUCKET=${MINIO_BUCKET:-heartbeats} \
            docker-compose up -d
    fi
}

minio-stop() {
    docker-compose down
    if [[ ${1} == "drop" ]];
        then
            volume=$(docker volume ls | grep bucket-data | awk '{print $2}')
            docker volume rm $volume
    fi
}
