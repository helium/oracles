# """A stupid python daemon to simulate 5g cell heartbeats
#
# From the poc5g rust server:
# https://github.com/novalabsxyz/poc5g-server/blob/main/src/heartbeat.rs
#
# pub struct CellHeartbeat {
#    #[serde(alias = "pubKey")]
#    pub pubkey: PublicKey,
#    pub hotspot_type: String,
#    pub cell_id: i32, # can be be between 1 and 3 cells per gateway
#    pub timestamp: DateTime<Utc>,
#    #[serde(alias = "longitude")]
#    pub lon: f64,
#    #[serde(alias = "latitude")]
#    pub lat: f64,
#    pub operation_mode: bool, cycle on and off
#    pub cbsd_category: String,
#
#    #[serde(skip_deserializing)]
#    pub id: Uuid,
#    #[serde(skip_deserializing)]
#    pub created_at: Option<DateTime<Utc>>,
# }"""

from binascii import crc32
from decimal import Decimal
import json
import logging
import os
from pprint import pformat
from random import choice, randrange, uniform
import re
import ssl
import time
import uuid
import urllib.request

log = logging.getLogger()


# some fake longitude and latitudes, all in bodies of water:
FAKE_LAT_LONG = {
    "pacific": (Decimal(35.160337), Decimal(204.104148)),
    "indian": (Decimal(-6.680975), Decimal(73.765083)),
    "baltic": (Decimal(56.605618), Decimal(19.511562)),
    "atlantic": (Decimal(51.615458), Decimal(-17.690478)),
    "so-atlantic": (Decimal(-35.604556), Decimal(-54.880125)),
}


def choose_mode():
    return uniform(0, 1) < 0.75


def build_gateways():
    # encode keys as bytes
    gateways = []
    for x in re.split(",", os.environ["5G_GATEWAYS"]):
        # generate an index into the latlong based on the pubkey
        # using crc32 modulo the length of the dict as a
        # consistent hash
        i = (crc32(x.encode()) & 0xFFFFFFFF) % len(FAKE_LAT_LONG)
        # between 1 and 3 cell ids
        cell_ids = [
            randrange(10000, 99999) for _ in range(max(1, min(3, (i % 3) + 1)))
        ]
        # use the index to select the lat, long coordinates
        (lat, lon) = FAKE_LAT_LONG[list(FAKE_LAT_LONG)[i]]
        gateways.append(
            {"gw_addr": x, "lat": lat, "lon": lon, "cell_ids": cell_ids}
        )

    return gateways


def main(gateways):
    for gw in gateways:
        heartbeat = {
            "pubkey": gw["gw_addr"],
            "hotspot_type": "type",
            "cell_id": choice(gw["cell_ids"]),
            "timestamp": int(time.time()),
            "lon": gw["lon"],
            "lat": gw["lat"],
            "operational_mode": choose_mode(),
            "cbsd_category": "category",
            "id": uuid.uuid4(),
            "created_at": int(time.time()),
        }
        context = ssl.create_default_context()
        data = json.dumps(heartbeat)
        hdrs = {
            "User-Agent": "heartbeat-daemon-1",
            "API-Key": os.environ["5G_API_KEY"],
        }
        req = urllib.request.Request(
            os.environ["5G_HEARTBEAT_URL"],
            data=data,
            headers=hdrs,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30, context=context) as resp:
            status = resp.get_code()
            if status > 199 and status < 300:
                log.info(
                    "{} status for gw: {}".format(status, heartbeat["pubkey"])
                )
            else:
                log.error(
                    "{} status request data {}".format(status, pformat(req))
                )


if __name__ == "__main__":
    gateways = build_gateways()
    main(gateways)
