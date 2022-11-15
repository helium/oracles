#!/usr/bin/env python3
# """A stupid python daemon to simulate beacon and witness reports
#
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

import lora_pb2
import lora_pb2_grpc
import time

from dotenv import load_dotenv

log = logging.getLogger()
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

PICKLE_FILE="lora_gateways.pickle"

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
    if os.path.exists("lora_gateways.pickle"):
        with open(PICKLE_FILE, "rb") as f:
            gateways = pickle.load(f)
    else:
        gateways = build_gateways_from_env([])
        with open(PICKLE_FILE, "wb") as f:
            pickle.dump(gateways, f, pickle.HIGHEST_PROTOCOL)

    if gateways:
        return gateways
    else:
        log.critical("Could not build gateways. Is LORA_GATEWAYS set?")
        os.exit(1)

def build_gateways_from_env(gateways):
    for x in re.split(",", os.environ["LORA_GATEWAYS"]):
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
        ts = time.time(),
        print(ts),
        rand_id = uuid.uuid4(),
        beaconer_pubkey = uuid.uuid4(),
        witness_pubkey = uuid.uuid4(),
        beacon_id = str(rand_id[0]),
        log.debug('Your UUID is: ' + str(rand_id[0])),
        beacon = lora_pb2.lora_beacon_report_req_v1(
            pub_key=str.encode(str(beaconer_pubkey[0])),
	    beacon_id=str.encode(str(rand_id[0])),
	    local_entropy=str.encode('entropy1'),
	    remote_entropy=str.encode('entropy2'),
	    data=str.encode('datapayload'),
	    frequency=2.3,
	    channel=2,
	    datarate='SF7BW125',
	    tx_power=3,
	    timestamp=int(ts[0]),
	    signature=str.encode(gw['gw_addr'])
        )
        witness = lora_pb2.lora_witness_report_req_v1(
            pub_key=str.encode(str(witness_pubkey[0])),
            beacon_id=str.encode(str(rand_id[0])),
            packet=str.encode('datapayload'),
            timestamp=int(ts[0]),
	    ts_res=5,
	    signal=3,
	    snr=2,
	    frequency=2.3,
            channel=2,
            datarate='SF7BW125',
            signature=str.encode(gw['gw_addr'])
        )
       	log.debug("{}".format(pformat(beacon)))
        log.debug("{}".format(pformat(witness)))

        url = os.environ["LORA_GRPC_URL"]
        api_token = "Bearer " + os.environ["API_TOKEN"]

        credentials = grpc.ssl_channel_credentials()
        metadata = [('authorization', api_token)]
        with grpc.insecure_channel(target=url,
                                 options=[('grpc.lb_policy_name', 'pick_first'),
                                        ('grpc.enable_retries', 0),
                                        ('grpc.keepalive_timeout_ms', 10000)
                                       ]) as channel:
            stub = lora_pb2_grpc.poc_loraStub(channel)
            beacon_response = stub.submit_lora_beacon(beacon,metadata=metadata)
            witness_response = stub.submit_lora_witness(witness,metadata=metadata)
            print("submit beacon req received response: " + beacon_response.id)
            print("submit witness req received response: " + witness_response.id)

if __name__ == "__main__":

    # pull config from .env
    # if set, use the location given by this var
    if 'LORA_ENV_FILE' in os.environ:
        log.debug("loading .env from {}".format(os.environ["LORA_ENV_FILE"]))
        load_dotenv(os.environ["LORA_ENV_FILE"])
    else:
        # otherwise look in current working directory
        load_dotenv()

    gateways = build_gateways()

    main(gateways)
