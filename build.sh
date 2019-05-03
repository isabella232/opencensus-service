#!/bin/bash

set -e
DOCKER_TAG=${1:-$PIPA_IMAGE_FULL_NAME}
if [[ -z "${DOCKER_TAG}" ]]; then
    echo "$0 [image]"
    exit 1
fi

pipa build -x --push -- docker build -f ./cmd/occollector/Dockerfile.shopify -t $DOCKER_TAG .
pipa vouch
