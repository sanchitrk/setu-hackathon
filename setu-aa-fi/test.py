from app import MongoStorage, SetuFiData

MOCK_DATA = {"workflowId": "8afb78db-5f5e-4afe-92fb-968ce00bea57"}

if __name__ == "__main__":
    workflow_id = MOCK_DATA["workflowId"]
    storage = MongoStorage()
    workflow_item_doc = storage.get_workflow_item(workflow_id=workflow_id)

    setu_fi_data = SetuFiData(storage=storage, workflow_item=workflow_item_doc)
    setu_fi_data.process_fi_encrypted_data()
