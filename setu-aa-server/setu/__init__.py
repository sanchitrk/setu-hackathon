import json
import uuid

import jwt
import pendulum
import requests
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
from pymongo import MongoClient

from .config import (
    MONGODB_HOST,
    MONGODB_NAME,
    MONGODB_PWD,
    MONGODB_USER,
    RAHASYA_BASE_URL,
    SETU_CLIENT_API_KEY,
    SETU_SANDBOX_BASE_URL,
)

publisher = pubsub_v1.PublisherClient()


PROJECT_ID = "serengeti-development"
AA_FI_READY_TOPIC = "pub-aa-fi-ready"

MONGO_URL = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PWD}@{MONGODB_HOST}/{MONGODB_NAME}?retryWrites=true&w=majority"

# see https://github.com/dcrosta/flask-pymongo/issues/87
mongodb = MongoClient(MONGO_URL, connect=False)[MONGODB_NAME]

app = Flask(__name__)

#
# ideally this should be stored in ENV
PRIVATE_KEY = b"""-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCqFJU9fNL+OFOr
VMsbvb1hzHEJcNYVBTdngdm4UbiVO966QrLJGlZyDhI/UPGzecqzH/pK2R9Dyb57
6iEL+yFKqyStidqY0QOzNLf39GGerZqT9AS/M70T2feaqU+MrtTF6DCNjdTLjWbJ
CuITY2T3eDMjL8VD+c7v20fS2P1Ej5d3oGfYpQy53puvvU7BnnwCYLwfO6sW1h22
yrgC7IEbYAgf506JjNb/tnaAfSjyXSTZOvbwTCVM9lB03nEBWDctWsjX7MrABXgE
/0cOnBoerrb247+rWFA3EjjErVdfp74oxEkhg+Gwn8OzoOkliyawdHjkppyTM3Lb
qVq3alzrAgMBAAECggEAB9ESWtUVzWFBVyp6ezfpMEl5GHn7HNQ0i6lIHFSFKzap
Md5G4T84od1LsWVC9oCudDUQB8iayz7GZmOISUitawUalm8zgLp6dQ4DNn4gzm4k
IJCidzclhXgLDSyi31BZrw7QzHCsSv6grrS1VzbGOSlpEEDtzAX7IMLvNh2K5k+v
nT7JnfSa3oGAtV2sQbDCu3pkBuMLwXpevqw89D/w80uv3THoRzeedibI96/6eUXZ
AVd/Wn6wMTLieT0AHgueamiSVgxFMj6HojemVSvAZYewHBzSTBv1SmPU33Vwc7WR
6W/mTfdRUy1/ShkJUae5vkD6GmESBdq/0HKVIuDzAQKBgQD71IcQJ8WJQ906MLrF
CmMRDdCxVQ3+1y0P5QX+jSXQgWx9BTX++4n/4sf+VIBaDhS5wgUMCxhnx1QCpm64
S9CLcXAzCwI/Pmp8KI99cCxPEuO2RIM9V7/mxDC+WK3YYWGzzXBr35p6DMMcyNPm
JY0aLud5txMO1IH1HaERE+PvSwKBgQCs5YeXM+n7jBJyQECdfQKbNhUDiq6jXWW4
Zb5L8nhk2ibzZrI0mes9S2blF9lEaouKGN4lM4/8FjOAHKzNMEhlhvdw3FVcYE+w
qdduxrdk8fKj5qdb1Jfz9VXP0MYMUxhYopFr40LgzGs7LhZ+3IvecQA3qxjKAvZd
LFHtYbuk4QKBgQD10wJ9DLXRRoPf182ZpogKD7hWQrbEu3trdp8hWts86/nhGIMb
AqQ1O0UKyaX5QqGMqw6OMQ6Dz5n8dEbEdI2AcR2bVfW9ksoTpOxdAHDgR4otVDfg
W5YiSAVk6d/Zx0W+ZJ6HTuDWnzqfEJN4p9NTadHfiIx7/4lUorWlnAr2fQKBgGI7
95IdWPAnYcOwZgYVJQny7HWasib3xffDNyAHoAgNOtxImS/x1Ap7cPbxWezZbHcG
MhGI/mIIazJ7GGNs73Vf/e8OASH/RsfleBXkqgacwXQGdUhjvgJKfnsY763I+KhD
lcRq13DKNJLnWLizrnSwV6NJf0gn7rp5mAL76JWBAoGBAMjeTLgeO+NLNlpk/v7M
Yv2fqYMu+GzUWgfmlxhruf3z/v6a5TpO36jtWDUrFSB/5GH5H33yTQcUZE2q+RHl
Dv1KEvpuOl0HzD+nBaaXexRpagmSabgvm9NgiAaVrU1zAmYTAeNxtaeTqqPBXHxI
ZEviqjFASNegHiuW7zxvAOj+
-----END PRIVATE KEY-----"""


def makeDetachedJWS(payload, key):
    encoded = jwt.encode(payload, key, algorithm="RS256")
    splittedJWS = encoded.split(".")
    splittedJWS[1] = ""
    return ".".join(splittedJWS)


def get_timestamp():
    timestamp = pendulum.now(tz="UTC").to_iso8601_string()
    return timestamp


def get_txnid():
    r = str(uuid.uuid4())
    return r


def get_fi_data_range_from_consent(consent_item):
    consent_detail = consent_item["ConsentDetail"]
    fi_data_range = consent_detail["FIDataRange"]
    data_range_start = fi_data_range["from"]
    data_range_end = fi_data_range["to"]
    return data_range_start, data_range_end


def generate_fi_request_payload(
    consent_id, signed_digital_signature, key_material, data_range_start, data_range_end
):
    return {
        "ver": "1.0",
        "timestamp": get_timestamp(),
        "txnid": get_txnid(),
        "FIDataRange": {
            "from": data_range_start,
            "to": data_range_end,
        },
        "Consent": {
            "id": consent_id,
            "digitalSignature": signed_digital_signature,
        },
        "KeyMaterial": key_material,
    }


@app.route("/")
def ok():
    return "ok, all good!"


@app.route("/-/2", methods=["POST"])
def get_signed_consent():
    """
    for the consent id, we'll get the signed consent and store the signed consent as is
    in our database workflow item.
    This signed consent later used for next steps
    """
    data = request.get_json(force=True)
    print("--- received data ---")
    print(data)
    print("---------------------")

    workflow_id = data["workflow_id"]
    projection = {"consentFlow": True, "_id": False, "userRef": True}
    workflow_item_doc = mongodb.get_collection("aaSetuWorkflows").find_one(
        {"workflowId": workflow_id}, projection=projection
    )
    if not workflow_item_doc:
        print("oops this should not happen!")
        raise ValueError("consent handle or workflow item does not exists.")

    user_ref = workflow_item_doc["userRef"]
    print(f"get signed consent for workflowId {workflow_id} and userRef {user_ref}")

    consent_flow = workflow_item_doc["consentFlow"]
    consent_id = consent_flow["consentId"]
    payload = {}
    headers = {"client_api_key": SETU_CLIENT_API_KEY, "x-jws-signature": ""}
    url = f"{SETU_SANDBOX_BASE_URL}/Consent/{consent_id}"
    response = requests.request("GET", url, headers=headers, data=payload)
    signed_consent_response = response.json()
    print("----- SETU API RESPONSE -----")
    print(signed_consent_response)
    print("----- SETU RESPONSE END -----")
    signed_consent_hash = signed_consent_response["signedConsent"]

    update_fields = {"consentFlow.signedConsent": signed_consent_hash}
    result = mongodb.get_collection("aaSetuWorkflows").update_one(
        {"workflowId": workflow_id, "userRef": user_ref},
        {"$set": update_fields},
    )
    print(f"updated collection aaSetuWorkflows matched count: {result.matched_count}")
    return jsonify({"workflow_id": workflow_id})


@app.route("/-/3", methods=["POST"])
def get_rahasya_key():
    """
    Now we are in the DataFlow user state of the AA consent flow
    """
    data = request.get_json(force=True)
    print("--- received data ---")
    print(data)
    print("---------------------")

    workflow_id = data["workflow_id"]
    url = f"{RAHASYA_BASE_URL}/ecc/v1/generateKey"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    rahasaya_response = response.json()
    print("----- RAHASYA API RESPONSE -----")
    print(rahasaya_response)
    print("----- RAHASYA RESPONSE END -----")
    rahasaya_key_material = rahasaya_response["KeyMaterial"]
    rahasaya_private_key = rahasaya_response["privateKey"]
    #
    # update the database with the generated rahasya response
    data_flow = {"privateKey": rahasaya_private_key, "keyMaterial": rahasaya_key_material}
    update_fields = {"dataFlow": data_flow}  # new data flow object
    result = mongodb.get_collection("aaSetuWorkflows").update_one(
        {"workflowId": workflow_id},
        {"$set": update_fields},
    )
    print(f"updated collection aaSetuWorkflows matched count: {result.matched_count}")
    return jsonify({"workflow_id": workflow_id})


@app.route("/-/4", methods=["POST"])
def request_fi_data():
    """
    At this stage, all the actors FIP, AA FIU are aware of the user consent.
    Now we request for the financial information, for the consent - this makes sure the
    FIP starts to prepare the data at their end.


    Note:
        also create a async tasks with scheduled time to call for prepared/ready data
        helpful when the POST /FI/Notification webhook doesn't work
    """
    data = request.get_json(force=True)
    print("--- received data ---")
    print(data)
    print("---------------------")
    workflow_id = data["workflow_id"]
    projection = {
        "_id": False,
        "userRef": True,
        "consentItem": True,
        "consentFlow": True,
        "dataFlow": True,
    }
    workflow_item_doc = mongodb.get_collection("aaSetuWorkflows").find_one(
        {"workflowId": workflow_id}, projection=projection
    )
    if not workflow_item_doc:
        print("oops this should not happen!")
        raise ValueError("consent handle or workflow item does not exists.")

    consent_item = workflow_item_doc["consentItem"]
    consent_flow = workflow_item_doc["consentFlow"]
    data_flow = workflow_item_doc["dataFlow"]

    consent_id = consent_flow["consentId"]
    signed_consent = consent_flow["signedConsent"]
    key_material = data_flow["keyMaterial"]
    signed_consent_digital_signature = signed_consent.split(".")[-1]
    data_range_start, data_range_end = get_fi_data_range_from_consent(consent_item)
    payload = generate_fi_request_payload(
        consent_id=consent_id,
        signed_digital_signature=signed_consent_digital_signature,
        key_material=key_material,
        data_range_start=data_range_start,
        data_range_end=data_range_end,
    )

    headers = {
        "client_api_key": SETU_CLIENT_API_KEY,
        "x-jws-signature": makeDetachedJWS(payload, PRIVATE_KEY),
        "Content-Type": "application/json",
    }
    url = f"{SETU_SANDBOX_BASE_URL}/FI/request"
    payload = json.dumps(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    fi_data_request_response = response.json()
    session_id = fi_data_request_response["sessionId"]
    update_fields = {"dataFlow.sessionId": session_id}
    result = mongodb.get_collection("aaSetuWorkflows").update_one(
        {"workflowId": workflow_id},
        {"$set": update_fields},
    )
    print(f"updated collection aaSetuWorkflows matched count: {result.matched_count}")
    return jsonify({"workflow_id": workflow_id})


@app.route("/taskhandler", methods=["POST"])
def taskhandler():
    """
    Yet to be implemented, this will be used to trigger a pub sub event that will
    contain the workflowId that identifies the workflow states.
    This will get inturn trigger subscriber that will process the fi ready data.
    Note: that this is used in case POST /FI/Notification is not triggered by SETU
    """
    data = request.get_json(force=True)
    print("--- received data via async task ---")
    print(data)
    print("---------------------")
    workflow_id = data["workflow_id"]
    pubsub_data = {"workflowId": workflow_id}
    topic_path = publisher.topic_path(PROJECT_ID, AA_FI_READY_TOPIC)
    pubsub_data_json_string = json.dumps(pubsub_data).encode("utf-8")
    future = publisher.publish(topic_path, pubsub_data_json_string)
    print(future.result())
    print(f"Published message to topic {topic_path}.")
    return jsonify({"workflow_id": workflow_id})
