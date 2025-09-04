#!/usr/bin/env bash

set -euo pipefail

cd $GITHUB_WORKSPACE

# If a release version is provided via workflow_dispatch, use it
if [ -n "$RELEASE_VERSION" ]; then
    VERSION="$RELEASE_VERSION"
elif [ -z "$GITHUB_REF" ]; then
    git config --global --add safe.directory "$GITHUB_WORKSPACE"
    VERSION=$(git describe)
else
    VERSION=$(echo "$GITHUB_REF" | sed 's|refs/tags/||')
fi

echo "Building Debian package with version: $VERSION"

write_unit_template()
{
    local ORACLE=$1

    cat << -EOF >"/tmp/$ORACLE.service"
[Unit]
Description=$ORACLE
After=network.target
StartLimitInterval=60
StartLimitBurst=3

[Service]
Type=simple
ExecStart=/opt/$ORACLE/bin/$ORACLE -c /opt/$ORACLE/etc/settings.toml server
User=helium
PIDFile=/var/run/$ORACLE
Restart=always
RestartSec=15
WorkingDirectory=/opt/$ORACLE

### Remove default limits from a few important places:
LimitNOFILE=infinity
LimitNPROC=infinity
TasksMax=infinity

[Install]
WantedBy=multi-user.target
-EOF
}

write_prepost_template()
{
    local ORACLE=$1

    cat << -EOF >"/tmp/$ORACLE-preinst"
# add system user for file ownership and systemd user, if not exists
useradd --system --home-dir /opt/helium --create-home helium || true
-EOF

    cat << -EOF >"/tmp/$ORACLE-postinst"
# add to /usr/local/bin so it appears in path
ln -s /opt/$ORACLE/bin/$ORACLE /usr/local/bin/$ORACLE || true
-EOF

    cat << -EOF >"/tmp/$ORACLE-postrm"
rm -f /usr/local/bin/$ORACLE
-EOF
}

run_fpm()
{
    local ORACLE=$1
    local CONF_PATH=$2
    local VERSION=$3

    # XXX HACK fpm won't let us mark a config file unless
    # it exists at the specified path
    mkdir -p /opt/${ORACLE}/etc
    touch /opt/${ORACLE}/etc/settings.toml

    fpm -n ${ORACLE} \
        -v "${VERSION}" \
        -s dir \
        -t deb \
        --deb-systemd "/tmp/${ORACLE}.service" \
        --before-install "/tmp/${ORACLE}-preinst" \
        --after-install "/tmp/${ORACLE}-postinst" \
        --after-remove "/tmp/${ORACLE}-postrm" \
        --deb-no-default-config-files \
        --deb-systemd-enable \
        --deb-systemd-auto-start \
        --deb-systemd-restart-after-upgrade \
        --deb-user helium \
        --deb-group helium \
        --config-files /opt/${ORACLE}/etc/settings.toml \
        target/release/${ORACLE}=/opt/${ORACLE}/bin/${ORACLE} \
        $CONF_PATH=/opt/${ORACLE}/etc/settings-example.toml

    # copy deb to /tmp for upload later
    cp *.deb /tmp

}

# install fpm
apt update
apt install --yes ruby
gem install public_suffix -v 5.1.1
gem install rchardet -v 1.8.0
gem install dotenv -v 2.8.1
gem install fpm -v 1.14.2 # current as of 2022-11-08
echo "ruby deps installed" 

for config_path in "reward_index" "price" "ingest" "iot_config" "iot_packet_verifier" "iot_verifier" "poc_entropy"
do
    oracle=$(echo $config_path | sed -E 's!\./([^/]+)/.+$!\1!' | sed -E 's!_!-!g')
    
    echo "starting  $oracle $config_path $VERSION"
    write_unit_template $oracle
    echo "write_unit_template  $oracle done"
    write_prepost_template $oracle
    echo "write_prepost_template  $oracle done"
    run_fpm $oracle $config_path $VERSION
    echo "run_fpm  $oracle done"
done

for deb in /tmp/*.deb
do
    echo "uploading $deb"
    curl -u "${PACKAGECLOUD_API_KEY}:" \
         -F "package[distro_version_id]=210" \
         -F "package[package_file]=@$deb" \
         https://packagecloud.io/api/v1/repos/helium/oracles/packages.json
done
