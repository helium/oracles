#!/usr/bin/env bash

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

MOBILE_REWARDS_PKGNAME="mobile-rewards_${RELEASE_VERSION}_amd64.deb"

mv mobile_rewards.deb $MOBILE_REWARDS_PKGNAME

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

MOBILE_INGEST_PKGNAME="mobile-ingest_${RELEASE_VERSION}_amd64.deb"

mv mobile_ingest.deb $MOBILE_INGEST_PKGNAME
