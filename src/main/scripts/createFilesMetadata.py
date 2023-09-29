import os

# import the cognite-python-sdk client
from cognite.client import CogniteClient

class FilesMetadataGenerator:
    @classmethod
    def __init__(cls):
        cls.project = os.environ["PROJECT"]
        cls.clientName = os.environ["COGNITE_CLIENT_NAME"]
        cls.baseUrl = os.environ["COGNITE_BASE_URL"]
        cls.client = CogniteClient(project=cls.project, client_name=cls.clientName, base_url=cls.baseUrl)

    def upload_file(self, name):
        upload_link = self.client.files.upload(
            name= name,
            path = "testfile.txt",
            source = "spark datasource upsert test",
            mime_type = "text/plain"
        )

fmg = FilesMetadataGenerator()

for i in range(0,10):
    print("Creating file " + str(i))
    fmg.upload_file("test file " + str(i))
