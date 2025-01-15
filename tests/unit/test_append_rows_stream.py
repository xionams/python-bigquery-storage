import unittest
from google.cloud import bigquery_storage_v1
from google.api_core.exceptions import GoogleAPICallError
import google.auth

class TestBigQueryStorage(unittest.TestCase):
    def setUp(self):
        self.client = bigquery_storage_v1.BigQueryWriteClient()
        self.project_id = "unique-atom-416219"
        self.dataset_id = "testone"
        self.table_id = "testtable"
        
        self.write_stream = f"projects/{self.project_id}/datasets/{self.dataset_id}/tables/{self.table_id}/streams/_default"

    def test_create_write_stream(self):
        try:
            parent = f"projects/{self.project_id}/datasets/{self.dataset_id}/tables/{self.table_id}"
            write_stream = bigquery_storage_v1.types.WriteStream()
            response = self.client.create_write_stream(parent=parent, write_stream=write_stream)
            self.assertIsNotNone(response)
            print(f"Write stream created: {response.name}")

        except GoogleAPICallError as e:
            self.fail(f"Google API call failed: {e}")

if __name__ == '__main__':
    unittest.main()
