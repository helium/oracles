#!/usr/bin/env python3
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
# }
#
# {
#   "cbsd_category": "A",
#   "cell_id": 630262,
#   "created_at": "2022-06-29T20:09:41.321063Z",
#   "hotspot_type": "enodeb",
#   "id": "6906f158-f7e7-11ec-a998-63127488bd46",
#   "lat": 27.901068,
#   "lon": -82.739204,
#   "operation_mode": true,
#   "pubkey": "112HR2gQ2LimfGvYHRrVxUCuMAWhrW8h7NY25Nefo8yQfR1RwbNj",
#   "timestamp": "2022-06-28T17:00:28Z"
#}
#
#
# """

import base64
from binascii import crc32
import pickle
from decimal import Decimal
import json
import logging
import os
from pprint import pformat, pprint
from random import choice, randrange, uniform
import re
import ssl
import uuid

import grpc

import heartbeat_pb2
import heartbeat_pb2_grpc

from dotenv import load_dotenv
from datetime import datetime

log = logging.getLogger()
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

PICKLE_FILE="heartbeat_gateways.pickle"

FCC_IDS = [ '2AG32PBS3101S', '2AG32MBS3100196N',
            '2AG32PBS31010', 'P27-SCE4255W',
            'P27-SCO4255PA10' ]

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
    if os.path.exists("heartbeat_gateways.pickle"):
        with open(PICKLE_FILE, "rb") as f:
            gateways = pickle.load(f)
    else:
        gateways = build_gateways_from_env([])
        with open(PICKLE_FILE, "wb") as f:
            pickle.dump(gateways, f, pickle.HIGHEST_PROTOCOL)

    if gateways:
        return gateways
    else:
        log.critical("Could not build gateways. Is HEARTBEAT_GATEWAYS set?")
        os.exit(1)

def build_gateways_from_env(gateways):
    for x in re.split(",", os.environ["HEARTBEAT_GATEWAYS"]):
        # generate an index into the latlong based on the pubkey
        # using crc32 modulo the length of the dict as a
        # consistent hash
        i = crc32(x.encode()) & 0xffffffff
        i_ll = i % len(FAKE_LAT_LONG)
        i_fcc = i % len(FCC_IDS)

        fcc_id = FCC_IDS[i_fcc]

        serial = base64.b32encode((i & 0x0000ffff).to_bytes(4, 'big')).decode().replace('=','') + str(i & 0xffff0000)
        # between 1 and 3 cell ids
        cell_ids = [
            randrange(100000, 999999) for _ in range(max(1, min(3, (i % 3)+1)))
        ]
        # use the index to select the lat, long coordinates
        (lat, lon) = FAKE_LAT_LONG[list(FAKE_LAT_LONG)[i_ll]]
        gateways.append(
            {"gw_addr": x, "lat": lat, "lon": lon,
             "cell_ids": cell_ids, "fcc_id": fcc_id,
             "serial": serial}
        )

    return gateways

def main(gateways):
    for gw in gateways:
        curr_dt = datetime.now(),
        heartbeat = heartbeat_pb2.cell_heartbeat_req_v1(
            pub_key=str.encode(gw['gw_addr']),
            hotspot_type='enodeb',
            cell_id=choice(gw['cell_ids']),
            timestamp=1000000,
            lon=float(gw['lon']),
            lat=float(gw['lat']),
            operation_mode=choose_mode(),
            cbsd_category='A',
            cbsd_id=gw['fcc_id'] + gw['serial']
        )
        #log.debug("{}".format(pformat(heartbeat)))
        
        url = os.environ["HEARTBEAT_GRPC_URL"]
        api_token = grpc.access_token_call_credentials("API_TOKEN")
                                        
        credentials = grpc.ssl_channel_credentials()
        metadata = [('authorization', api_token)]
        with grpc.insecure_channel(target=url,
                                 options=[('grpc.lb_policy_name', 'pick_first'),
                                        ('grpc.enable_retries', 0),
                                        ('grpc.keepalive_timeout_ms', 10000)
                                       ]) as channel:
            stub = heartbeat_pb2_grpc.poc_mobileStub(channel)
            response = stub.submit_cell_heartbeat(heartbeat,metadata=metadata)

            print("submit heartbeat req received response: " + response.message)        


if __name__ == "__main__":
    
    # pull config from .env
    # if set, use the location given by this var
    if 'HEARTBEAT_ENV_FILE' in os.environ:
        log.debug("loading .env from {}".format(os.environ["HEARTBEAT_ENV_FILE"]))
        load_dotenv(os.environ["HEARTBEAT_ENV_FILE"])
    else:
        # otherwise look in current working directory
        load_dotenv()

    gateways = build_gateways()

    main(gateways)