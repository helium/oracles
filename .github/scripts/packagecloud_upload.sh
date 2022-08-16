
MOBILE_REWARDS_PKGNAME="mobile-rewards_${RELEASE_VERSION}_amd64.deb"
MOBILE_INGEST_PKGNAME="mobile-ingest_${RELEASE_VERSION}_amd64.deb"

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_REWARDS_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/mobile_rewards/packages.json

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_INGEST_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/mobile_ingest/packages.json
