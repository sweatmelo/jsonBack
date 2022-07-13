from hashlib import sha3_384
import boto3
from botocore.config import Config
from botocore.utils import fix_s3_host
from boto3.session import Session
import os
import pathlib
from datetime import datetime
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from DatabaseHandler import DatabaseHandler
import json

"""
my_config = Config(
    region_name = '',
    s3 = {
        'addressing_style': 'path'
    },
    retries = {
        'max_attempts': 2,
        'mode': 'standard'
    }
)

session = Session(
    aws_access_key_id="4G8F4PBHBLNX7ZOW8N5P",
    aws_secret_access_key="N7u4Xwl6UUf6cV5y2G0KxWse6MC24VHjNHm8f0Lr"
)

s3 = session.resource(
    service_name="s3",
    config=my_config,
    endpoint_url='https://s3.cluster.predictive-quality.io'
    )

# s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
bucket = s3.Bucket("ggr-bucket-cbf77f1e-eea2-4b4a-88b2-ae787daf3f42")
client = s3.meta.client
prefix = 'SPP_Files/'
result = client.list_objects("ggr-bucket-cbf77f1e-eea2-4b4a-88b2-ae787daf3f42", Prefix=prefix, Delimiter='/')

#client.download_file("ggr-bucket-cbf77f1e-eea2-4b4a-88b2-ae787daf3f42", "out/test.txt", "test.txt")
"""
class FileServerHandler():

    Upload_path = ""
    Schema_path = ""
    database_handler = None

    def __init__(self,db_handler:DatabaseHandler):
        self.Upload_path = "files"
        self.Schema_path = "schemas"
        self.database_handler = db_handler
        #check if file directory already exists otherwise create it
        if not os.path.isdir(self.Upload_path):
            print("files dir doesnt exist")
            os.mkdir(self.Upload_path)
        #check if schema directory already exists otherwise create it
        if not os.path.isdir(self.Schema_path):
            print("schema dir doesnt exist")
            os.mkdir(self.Schema_path)
        #check if Files collection already exists otherwise create it
        db_handler.create_collection_if_not_exists("Files")

    def save(self, file:FileStorage):
        relative_file_path, filename = self._create_file_path(file.filename)
        file.save(relative_file_path)
        file.close()
        #create document to be saved in the Files collection
        json_file_document = {
            "name" : filename,
            "filepath" : relative_file_path,
            "creation_date" : datetime.today().strftime('%d.%m.%Y')
        }
        return self.database_handler.insert_object("Files", json_file_document)

    def _create_file_path(self, filename:str):
        secure_filename_with_extension = secure_filename(filename)
        secure_filename_without_extension, secure_name_extension = os.path.splitext(secure_filename_with_extension)
        final_filename = secure_filename_without_extension
        file_path = os.path.join(self.Upload_path, secure_filename_with_extension)
        if os.path.exists(file_path):
            counter = 1
            file_path = os.path.join(self.Upload_path, secure_filename_without_extension + "_" + str(counter) + secure_name_extension)
            while os.path.exists(file_path):
                counter += 1
                final_filename = secure_filename_without_extension + "_" + str(counter)
                file_path = os.path.join(self.Upload_path, final_filename + secure_name_extension)
        return file_path, final_filename

    def get_collection_names_from_folder(self):
        filenames = next(os.walk(self.Schema_path), (None, None, []))[2]  # [] if no file
        collection_names = []
        for filename_with_extension in filenames:
            filename, extension = os.path.splitext(filename_with_extension)
            collection_names.append(filename)
        return collection_names

    def read_schema_from_file(self, name:str):
        try:
            f = open(os.path.join(self.Schema_path, name + ".json"))
            schema = json.load(f)
            return schema
        except Exception as e:
            print(e)
            return {}

if __name__ == '__main__':
    pass
