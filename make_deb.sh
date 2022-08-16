#!/usr/bin/env bash

REPO=poc_5g

# Build mobile rewards server

mkdir mobile_rewards
mkdir mobile_rewards/DEBIAN
mkdir -p mobile_rewards/usr/bin

cat > mobile_rewards/DEBIAN/control <<-EOF
Package: mobile_rewards
Version: $RELEASE_VERSION
Section: custom
Priority: optional
Architecture: amd64
Essential: no
EOF

mv target/release/poc5g-rewards mobile_rewards/usr/bin/

dpkg-deb --build mobile_rewards

MOBILE_REWARDS_PKGNAME="mobile_rewards-${RELEASE_VERSION}-amd64.deb"

mv mobile_rewards.deb $MOBILE_REWARDS_PKGNAME

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_REWARDS_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/${REPO}/packages.json

# Build mobile ingest server

mkdir mobile_ingest
mkdir mobile_ingest/DEBIAN
mkdir -p mobile_ingest/usr/bin

cat > mobile_ingest/DEBIAN/control <<-EOF
Package: mobile_ingest
Version: $RELEASE_VERSION
Section: custom
Priority: optional
Architecture: amd64
Essential: no
EOF

mv target/release/poc5g-ingest mobile_ingest/usr/bin/

dpkg-deb --build mobile_ingest

MOBILE_INGEST_PKGNAME="mobile_ingest-${RELEASE_VERSION}-amd64.deb"

mv mobile_ingest.deb $MOBILE_INGEST_PKGNAME

curl -u "${PACKAGECLOUD_API_KEY}:" \
     -F "package[distro_version_id]=210" \
     -F "package[package_file]=@${MOBILE_INGEST_PKGNAME}" \
     https://packagecloud.io/api/v1/repos/helium/${REPO}/packages.json
