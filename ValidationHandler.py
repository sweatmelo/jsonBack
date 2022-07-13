from calendar import c
from gridfs import Database
from pkg_resources import require
import pymongo, os, json
from bson.json_util import dumps
from bson.objectid import ObjectId
from typing import Optional
from DatabaseHandler import DatabaseHandler
from FileServerHandler import FileServerHandler
import mongo_schema
import jsonschema

class ValidationHandler():

    database_handler : DatabaseHandler = None
    file_server_handler : FileServerHandler = None

    def __init__(self,db_handler:DatabaseHandler, file_server_handler:FileServerHandler):
        self.database_handler = db_handler
        self.file_server_handler = file_server_handler
        self.startup_collection_creation()
        
    
    def startup_collection_creation(self):
        db = self.database_handler.get_database()
        found_collections, existing_collections = self.database_handler.get_all_collections()
        if found_collections:
            existing_collections = existing_collections["collections"]  #get just the list from the json object TODO add check if collections have been found
        else:
            existing_collections = []
        collections_from_folder = self.file_server_handler.get_collection_names_from_folder()
        non_existing_collections = [coll for coll in collections_from_folder if coll not in existing_collections]

        #create collection that don't exists yet and add the respective schemas
        for coll_name in non_existing_collections:
            self.database_handler.create_collection_if_not_exists(coll_name)
            schema = self.file_server_handler.read_schema_from_file(coll_name)
            self.insert_schema(coll_name, schema)

        #overwrite schemas for existing_collections
        for coll_name in existing_collections:
            schema = self.file_server_handler.read_schema_from_file(coll_name)
            self.insert_schema(coll_name, schema)
        
        #insert default values for all missing values
        violating_documents = self.get_schema_violating_documents()
        for collection_name in violating_documents:
            try:    #set schema validation to warning instead of error to be able to add null values
                db.command("collMod", collection_name, validationAction="warn")
            except Exception as e:
                print(e)
            collection_schema = self.get_schema(collection_name)
            if "required" in collection_schema: #check if schema actually has required fields
                for violating_document_id in violating_documents[collection_name]:
                    found_doc, doc_data = self.database_handler.get_object_metadata(violating_document_id)
                    if found_doc:
                        for required_field in collection_schema["required"]:
                            if required_field not in doc_data:
                                doc_data[required_field] = None
                    self.database_handler.overwrite_object_metadata(violating_document_id,doc_data)
            try:    #set schema validation back to error
                db.command("collMod", collection_name, validationAction="error")
            except Exception as e:
                print(e)


    def get_schema_violating_documents(self):
        violating_documents = {}
        found_collections, existing_collections = self.database_handler.get_all_collections()
        existing_collections = existing_collections["collections"]  #get just the list from the json object TODO add check if collection have been found
        for coll_name in existing_collections:
            violating_documents[coll_name] = []
            coll_schema = self.get_schema(coll_name)
            not_empty, root_documents_list = self.database_handler.get_root_tree(coll_name)
            if not_empty:
                for doc in root_documents_list["documents"]:
                    id = doc["_id"]
                    found_doc, doc_data = self.database_handler.get_object_metadata(ObjectId(id))
                    if found_doc:
                        try:
                            mongo_schema.validate(doc_data, coll_schema)
                        except jsonschema.exceptions.ValidationError as e:
                            violating_documents[coll_name].append(id)   #TODO also add indication which fields are violating (can only be done for the "topmost" validation error so not all errors can be shown)
        return violating_documents

    def get_documents_with_null_fields(self):
        null_documents = {}
        found_collections, existing_collections = self.database_handler.get_all_collections()
        existing_collections = existing_collections["collections"]  #get just the list from the json object TODO add check if collection have been found
        for coll_name in existing_collections:
            null_documents[coll_name] = {}
            not_empty, root_documents_list = self.database_handler.get_root_tree(coll_name)
            if not_empty:
                for doc in root_documents_list["documents"]:
                    id_str = str(doc["_id"])
                    found_doc, doc_data = self.database_handler.get_object_metadata(ObjectId(id_str))
                    json_doc_data = self.database_handler.parse_bson(doc_data)
                    if found_doc:
                        for field in doc_data:
                            if isinstance(json_doc_data[field],type(None)):
                                if not id_str in null_documents[coll_name]:
                                    null_documents[coll_name][id_str] = [field]
                                else:
                                    null_documents[coll_name][id_str].append(field) 
        return null_documents

    def get_schema(self, collection_name:str): 
        db = self.database_handler.get_database()
        if self.database_handler.does_collection_exist(collection_name):    #check first if collection exists otherwise get_collection would create it, which is unwanted behavior
            collection = db.get_collection(collection_name)
            options = collection.options()
            if options and "validator" in options and "$jsonSchema" in options.get('validator'):
                return options.get('validator').get('$jsonSchema')
        return {}

    
    def insert_schema(self, collection_name:str, schema:dict):
        db = self.database_handler.get_database()
        try:
            db.command("collMod", collection_name, validator=schema)
        except Exception as e:
            print(e)

