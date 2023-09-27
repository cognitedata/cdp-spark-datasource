import sys
import random
import os
import json

# install cognite-python-sdk
from cognite import CogniteClient


class GenerateThreeDData:
    @classmethod
    def __init__(cls):
        cls.project = os.environ["PROJECT"]
        cls.client = CogniteClient(cls.project)
        cls.random_model = "model_{}".format(random.randint(0, sys.maxsize))

    def create_model(self):
        model_response = json.loads(
            self.client.post(
                "/api/0.6/projects/{}/3d/models".format(self.project), body={"items": [{"name": self.random_model}]}
            ).text
        )
        return model_response["data"]["items"][0]["id"]

    def create_revision(self, model_id):
        test_input = os.path.abspath("scene.fbx")
        file_response = self.client.files.upload_file(
            file_name=str(model_id) + ".fbx", file_path=test_input, content_type="application/octet-stream"
        )
        data = {"items": [{"fileId": file_response["fileId"]}]}
        revision_response = json.loads(
            self.client.post(
                "/api/0.6/projects/{}/3d/models/{}/revisions".format(self.project, model_id), body=data
            ).text
        )["data"]["items"]
        return revision_response[0]["id"]

    def create_mapping(self, model_id, revision_id):
        data = {"items": [{"nodeId": random.randint(0, sys.maxsize), "assetId": random.randint(0, sys.maxsize)}]}

        return json.loads(
            self.client.post(
                "/api/0.6/projects/{}/3d/models/{}/revisions/{}/mappings".format(self.project, model_id, revision_id),
                body=data,
            ).text
        )


generators = [GenerateThreeDData() for x in range(5)]

for g in generators:
    model = g.create_model()
    revision = g.create_revision(model)
    mapping = g.create_mapping(model, revision)
