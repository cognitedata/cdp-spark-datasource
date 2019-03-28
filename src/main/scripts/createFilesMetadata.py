import os

# import the cognite-python-sdk client
from cognite.client import CogniteClient

class FilesMetadataGenerator:
    @classmethod
    def __init__(cls):
        cls.apiKey = os.environ["TEST_API_KEY_WRITE"]
        cls.project = os.environ["PROJECT"]
        cls.client = CogniteClient(cls.apiKey, cls.project)

    def upload_file(self, file_name):
        upload_link = self.client.files.upload_file(
            file_name,
            file_path = "testfile.txt",
            directory = "testdata",
            source = "spark datasource upsert test",
            content_type = "text/plain"
        )

fmg = FilesMetadataGenerator()

for i in range(0,10):
    print("Creating file " + str(i))
    fmg.upload_file("test file " + str(i))