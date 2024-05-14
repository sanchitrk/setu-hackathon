import base64
import json

import jwt
import requests
from pymongo import MongoClient

from .config import (
    MONGODB_HOST,
    MONGODB_NAME,
    MONGODB_PWD,
    MONGODB_USER,
    PRIVATE_KEY,
    RAHASYA_URL,
    SETU_CLIENT_API_KEY,
    SETU_SANDBOX_BASE_URL,
)

# from .dummy import fi_fetch_dummy

MONGO_URL = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PWD}@{MONGODB_HOST}/{MONGODB_NAME}?retryWrites=true&w=majority"


def makeDetachedJWS(payload, key):
    encoded = jwt.encode(payload, key, algorithm="RS256")
    splittedJWS = encoded.split(".")
    splittedJWS[1] = ""
    return ".".join(splittedJWS)


def extract_equity_dataset(data: dict):
    linked_accout_ref = data["linkedAccRef"]
    print(f"extracting for linked account ref {linked_accout_ref}")
    required_data = []

    transactions = data["transactions"]
    transaction = transactions["transactions"]
    for tr in transaction:
        isin = tr["isin"]
        name = tr["companyName"]
        try:
            average_price = int(tr["strikePrice"])
        except:
            average_price = 200
        data = {"isin": isin, "name": name, "averagePrice": average_price}
        required_data.append(data)
    return required_data


def extract_mutual_funds(data: dict):
    linked_accout_ref = data["linkedAccRef"]
    print(f"extracting for linked account ref {linked_accout_ref}")
    required_data = []

    summary = data["summary"]
    investment = summary["investment"]
    holdings = investment["holdings"]
    holding = holdings["holding"]

    for hl in holding:
        isin = hl["isin"]
        name = hl["amc"] + hl["schemeCode"]
        try:
            average_price = int(hl["nav"])
        except:
            average_price = 56
        data = {"isin": isin, "name": name, "averagePrice": average_price}
        required_data.append(data)
    return required_data


class MongoStorage(object):
    def __init__(self):
        print("initialize mongo storage...")
        self.mongodb = MongoClient(MONGO_URL, connect=False)[MONGODB_NAME]

    def get_workflow_item(self, workflow_id):
        result = self.mongodb.get_collection("aaSetuWorkflows").find_one(
            {"workflowId": workflow_id}
        )
        return result

    def store_in_temp_collection(self, item):
        print("insert item in temp collection...")
        result = self.mongodb.get_collection("temp").insert_one(item)
        print(result.inserted_id)

    def update_user_linked_holdings(self, user_ref, item):
        account = item["account"]
        data_type = account["type"]
        if data_type == "equities":
            results = extract_equity_dataset(account)
            for result in results:
                result["userRef"] = user_ref
                self.mongodb.get_collection("linkedHoldings").insert_one(result)
        elif data_type == "mutual_funds":
            results = extract_mutual_funds(account)
            for result in results:
                result["userRef"] = user_ref
                self.mongodb.get_collection("linkedHoldings").insert_one(result)
        else:
            print("hmm, no match for data type, unsupported")

    def update_workflow_status(self, workflow_id, status):
        print("updating workflow status...")
        update_fields = {"workflowStatus": status}
        result = self.mongodb.get_collection("aaSetuWorkflows").update_one(
            {"workflowId": workflow_id},
            {"$set": update_fields},
        )
        print(f"updated collection aaSetuWorkflows matched count: {result.matched_count}")


class SetuFiData(object):
    def __init__(self, storage: MongoStorage, workflow_item: dict) -> None:
        self.storage = storage
        self.workflow_item = workflow_item

    @property
    def user_ref(self):
        return self.workflow_item["userRef"]

    @property
    def session_id(self):
        data_flow = self.workflow_item["dataFlow"]
        return data_flow["sessionId"]

    @property
    def key_material(self):
        data_flow = self.workflow_item["dataFlow"]
        return data_flow["keyMaterial"]

    @property
    def private_key(self):
        data_flow = self.workflow_item["dataFlow"]
        return data_flow["privateKey"]

    def _decode_base64_data(self, base64_data):
        fi_data = base64_data["base64Data"]
        decoded = base64.b64decode(fi_data)
        data_dict = json.loads(decoded)
        return data_dict

    def _decrypt_each_fi(self, item: dict):
        fip_id = item["fipId"]
        print(f"decrypting item for fipId {fip_id}")
        remote_key_material = item["KeyMaterial"]
        base64_remote_nonce = remote_key_material["Nonce"]

        base64_your_nonce = self.key_material["Nonce"]
        our_private_key = self.private_key

        remote_dataset = item["data"]
        for fi_data in remote_dataset:
            fi_base64_encrypted_data = fi_data["encryptedFI"]
            payload = {
                "base64Data": fi_base64_encrypted_data,
                "base64RemoteNonce": base64_remote_nonce,
                "base64YourNonce": base64_your_nonce,
                "ourPrivateKey": our_private_key,
                "remoteKeyMaterial": remote_key_material,
            }
            headers = {
                "client_api_key": SETU_CLIENT_API_KEY,
                "x-jws-signature": makeDetachedJWS(payload, PRIVATE_KEY),
                "Content-Type": "application/json",
            }
            print(headers)
            url = f"{RAHASYA_URL}/ecc/v1/decrypt"
            payload = json.dumps(payload)
            response = requests.request("POST", url, headers=headers, data=payload)
            fi_data_base64 = response.json()
            decoded_data = self._decode_base64_data(fi_data_base64)
            self.storage.store_in_temp_collection(decoded_data)
            try:
                self.storage.update_user_linked_holdings(self.user_ref, decoded_data)
            except Exception as exp:
                print(exp)
                print("error loading data into linked holdings - this should not happen")

    def process_fi_encrypted_data(self):
        headers = {"client_api_key": SETU_CLIENT_API_KEY, "x-jws-signature": ""}
        payload = {}
        url = f"{SETU_SANDBOX_BASE_URL}/FI/fetch/{self.session_id}"
        response = requests.request("GET", url, headers=headers, data=payload)
        encrypted_data = response.json()
        financial_information = encrypted_data["FI"]
        for fi in financial_information:
            self._decrypt_each_fi(fi)
        txnid = encrypted_data["txnid"]
        print(f"done for txnid {txnid}")
