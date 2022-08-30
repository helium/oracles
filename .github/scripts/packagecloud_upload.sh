#!/usr/bin/env bash

set -euo pipefail

MOBILE_REWARDS_PKGNAME=$( find target/debian -type f -iname "poc5g-rewards_*.deb" )
MOBILE_INGEST_PKGNAME=$( find target/debian -type f -iname "poc5g-ingest_*.deb" )
MOBILE_STATUS_PKGNAME=$( find target/debian -type f -iname "poc5g-status_*.deb" )

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_REWARDS_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/mobile_rewards/packages.json

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_INGEST_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/mobile_ingest/packages.json

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_STATUS_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/mobile_status/packages.json
