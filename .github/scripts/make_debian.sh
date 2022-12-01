#!/usr/bin/env bash

set -euo pipefail

cd $GITHUB_WORKSPACE

if [ -z "$GITHUB_REF" ]; then
    git config --global --add safe.directory "$GITHUB_WORKSPACE"
    VERSION=$(git describe)
else
    VERSION=$(echo "$GITHUB_REF" | sed 's|refs/tags/||')
fi


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
sudo apt update
sudo apt install --yes ruby
sudo gem install fpm -v 1.14.2 # current as of 2022-11-08

for config_path in $( find . -name 'settings-template.toml' )
do
    oracle=$(echo $config_path | sed -E 's!\./([^/]+)/.+$!\1!' | sed -E 's!_!-!g')

    write_unit_template $oracle
    write_prepost_template $oracle
    run_fpm $oracle $config_path $VERSION
done

for deb in /tmp/*.deb
do
    echo "uploading $deb"
    curl -u "${PACKAGECLOUD_API_KEY}:" \
         -F "package[distro_version_id]=210" \
         -F "package[package_file]=@$deb" \
         https://packagecloud.io/api/v1/repos/helium/oracles/packages.json
done
