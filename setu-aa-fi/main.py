import base64
import json

from app import MongoStorage, SetuFiData


def run_pub_sub(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubSub Message message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """

    print(
        """This Function was triggered by messageId {} published at {}
    """.format(
            context.event_id, context.timestamp
        )
    )

    if "data" in event:
        print("data found in event running function now...")
        data = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(data)
        print("----- RECIEVED PUBSUB DATA -----")
        print(data)
        print("---- END OF DATA ----")

        workflow_id = data["workflowId"]
        storage = MongoStorage()
        workflow_item_doc = storage.get_workflow_item(workflow_id=workflow_id)

        if workflow_item_doc["workflowStatus"] == "SUCCESS":
            print("workflow already a success.")
            return

        setu_fi_data = SetuFiData(storage=storage, workflow_item=workflow_item_doc)
        setu_fi_data.process_fi_encrypted_data()
        storage.update_workflow_status(workflow_id=workflow_id, status="SUCCESS")
    else:
        print("no data found in event doing nothing")
