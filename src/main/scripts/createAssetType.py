import os
import requests

# import the cognite-python-sdk client
from cognite.client import CogniteClient

class AssetTypeGenerator:
    @classmethod
    def __init__(cls):
        cls.apiKey = os.environ["TEST_API_KEY_WRITE"]
        cls.project = os.environ["PROJECT"]
        cls.client = CogniteClient(cls.apiKey, cls.project)

    def create_asset_type(self):
        header = {
            'Content-Type': 'application/json', 'API-key': self.apiKey
        }

        req_body = {
            "items":[
                {
                    "name":"pump",
                    "description": "Oil pump",
                    "fields":[
                        {
                            "name": "confidence",
                            "description": "The confidence level of the assigned asset type",
                            "valueType": "Double"
                        },
                        {
                            "name": "confidence",
                            "description": "The source that gives the asset its type: model, expert, manual, regexp",
                            "valueType": "String"
                        },
                        {
                            "name": "isTrue",
                            "description": "A test field for Boolean",
                            "valueType": "Boolean"
                        },
                        {
                            "name": "isTrue",
                            "description": "A test field for Longs",
                            "valueType": "Long"
                        }

                    ]
                }
            ]
        }

        url = 'https://api.cognitedata.com/api/0.6/projects/%s/assets/types' % self.project
        response = requests.post(url=url, headers=header, json=req_body)
        response.raise_for_status()

atg = AssetTypeGenerator()

atg.create_asset_type()
