import pymongo, os, json
from pymongo.errors import WriteError
from bson.json_util import dumps
from bson.objectid import ObjectId
from typing import Optional


class DatabaseHandler():

   db = None
   db_name = ""

   def __init__(self, url:str) -> None:
      try:
         client = pymongo.MongoClient(url, tlsAllowInvalidCertificates = True)
         self.db_name = "WZL"    #declare which database we wanna use
         self.db = client.get_database(self.db_name) 
      except Exception as e:
         print("Was not able to connect to MongoDB server: " + str(e))

   def get_all_collections(self):
      '''
      Lists all the collections in the database

      Returns:
         dict: json dict of all the collections in the database
      '''
      try:
         cols = self.db.list_collection_names()
      except Exception as e:
         return False, {"message":str(e)}
      return True, {"collections":cols}

   def get_root_tree(self, collection:str): #TODO dont return error if collection is empty
      try:
         if self.does_collection_exist(collection): 
            collection_obj = self.db.get_collection(collection)
            root_cursor = collection_obj.find()
            if not root_cursor.alive:
               return False, {"message":"The collection doesn't exist or is empty"}
            root_document_list = []
            for document in root_cursor:
               doc_entry = {}
               doc_entry["_id"] = document["_id"]
               doc_entry["name"] = document["name"] if ("name" in document) else ""
               root_document_list.append(doc_entry)
            return True, {"documents":root_document_list}
         return False, {"message":"Collection doesnt exist"}
      except Exception as e:
         return False, {"message":str(e)}


   def get_json_schema(self, collection:str):
      '''
      Get the bson schema of the given collection
      Parameters: 
         collection: The collection where we want the schema from
      Returns:
         dict: a json dict of the schema
      '''
      raw_collection_data = self.parse_bson(self.db.command("listCollections")) #with listCollections we get back a bson file so we then parse it to json and then to a dict

      all_validation_rules = raw_collection_data["cursor"]["firstBatch"] #the file however has [] in it which python sees as a list so we have a list in a dictionary

      for rules in all_validation_rules: #here wo go through each object in the list, these are all the different collections and a bit of weird Mongo stuff, and find the correct collection
         if (dict(rules))["name"] == collection:
            try:
               schema_properties = dict(rules)["options"]["validator"]["$jsonSchema"]["properties"]
               return True, self._extract_schema(schema_properties)
            except:
               return False, {"message":"No Schema found"}

      return False, {"message":"Unknown Error"}

            
   def get_subobjects(self, id:ObjectId):
      found_obj, obj = self.get_object_metadata(id)
      if found_obj:
         found_referenced_objs, referenced_objects_list = self._get_referenced_objects(obj,include_names=True,include_collection=True)
         if found_referenced_objs:
            return True, self._objectids_to_strings_rec({"documents":referenced_objects_list})
         else:
            return False, {"message":"unable to get subojects"}


   #returns the metadata of a specific document, you can also specify the collection
   def get_object_metadata(self, search_id: ObjectId, collection:Optional["str"] = "*", show_hidden_attributes:Optional["bool"] = False):
      '''
      Parameters:
         search_id: _id of the Object we want to find
         collection: Optional, the collection to search in
         show_hidden_attributes: Optional, True if the h_ attributes should not be removed from the output
      '''
      result = ""
      if collection == "*":
         collection_to_search = self.db.list_collection_names()
      else:
         collection_to_search = [collection]
      for collection in collection_to_search:
         to_search = self.db.get_collection(collection)
         try:
            result = to_search.find({"_id" : search_id})[0] #get first search result
            if not show_hidden_attributes:
               for key in self.parse_bson(result): #remove all the hidden attributes
                  if key[0:2] == "h_":
                     result.pop(key)
            return True, self._oid_dicts_to_ObjectIds_rec(result)
         except Exception as e:
            pass
      return False, "Could not find the ID" 


   def get_refrence_attributes_name(self, search_id:ObjectId):
      found_doc, doc_data = self.get_object_metadata(search_id)
      result = []
      if found_doc:
         for attr in doc_data:
            if isinstance(doc_data[attr],dict) and "referenced_collection" in doc_data[attr]:
               result.append(attr)
         return True, result
      else:
         return False, {"message": "Could not find the document"}

   def overwrite_object_metadata(self, search_id:ObjectId, object_json:dict):
      #remove _id field because that can't be overwritten in the databse
      if "_id" in object_json:
         object_json.pop("_id")
      #convert raw json with $oid to bson with ObjectId objects
      object_json = self._oid_dicts_to_ObjectIds_rec(object_json)

      found_document, current_document_data = self.get_object_metadata(search_id, show_hidden_attributes=True)
      if found_document:
         retrieving_references_successfull, existing_references = self._get_referenced_objects(current_document_data,include_collection=True)   #save old references
         if retrieving_references_successfull:
            for collection in self.db.list_collection_names():
               current_db = self.db.get_collection(collection)
               try:
                  result = current_db.replace_one({"_id": search_id}, object_json)  #actually replace document with new data
               except WriteError as e:
                  return False, {"message":"Document failed validation"}
               if result.modified_count == 1: 
                  retrieving_references_successfull, references_of_new_doc = self._get_referenced_objects(object_json,include_collection=True) #save new references
                  if retrieving_references_successfull:
                     #compare references and insert/delete backreferences accordingly
                     added_references = [ref for ref in references_of_new_doc if (ref not in existing_references)]  
                     removed_references = [ref for ref in existing_references if (ref not in references_of_new_doc)]
                     for reference in added_references:
                        collection_of_referenced_doc = self.db.get_collection(reference["collection_name"])
                        self._insert_backreference(search_id,reference["_id"],collection_of_referenced_doc)
                     for reference in removed_references:
                        collection_of_referenced_doc = self.db.get_collection(reference["collection_name"])
                        self._delete_backreference(search_id,reference["_id"],collection_of_referenced_doc)
                     return True, {"message": "overwrite sucessfull"}
      return False, {"message":"Document not found"}
         

   def insert_object(self, coll_name:str, object_json:dict):
      '''
      Parameters:
         coll_name: Colletion where we want to insert the Object
         object_json: JSON file which will be inserted into the Colletion
      Returns:
         status: Boolean, True if successed, False if it failed
         id_of_object: _id of the Object that was created
      '''
      current_db = self.db.get_collection(coll_name)

      #convert dicts with $oid to ObjectId objects
      object_json = self._oid_dicts_to_ObjectIds_rec(object_json)
      #insert object into db
      if self.does_collection_exist(coll_name): 
         try:
            result = current_db.insert_one(object_json)
         except WriteError as e:
            return False, {"message":"Document failed validation"}
         if result.inserted_id:
            to_refrence_id = result.inserted_id  #We get the id of the Object that was inserted
         
            #Add backreferences
            success, subdocs = self._get_referenced_objects(object_json,include_collection=True)
            if success:
               for obj in subdocs: #all the objects that we need to backrefrence
                  self._insert_backreference(to_refrence_id,obj["_id"],self.db.get_collection(obj["collection_name"]))
               return True, {"_id":ObjectId(to_refrence_id)}
            else:
               return False, {"message":"could not get referenced objects"}
         else:
            return False, {"message":"insert failed"}
      else:
         return False, {"message":"Collection doesnt exist"}

   def delete_document(self, id:ObjectId):
      for collection_name in self.db.list_collection_names():
         current_db = self.db.get_collection(collection_name)
         deletion_doc_found, _ = self.get_object_metadata(id, collection=collection_name, show_hidden_attributes=True)  #get the object that should be deleted to find references to that object
         if deletion_doc_found:
            reference_deletion_successful = self._delete_all_references(id)   #delete all references to the object that will be deleted
            if reference_deletion_successful:
               result = current_db.delete_one({"_id": id})    #actually delete the object
               if result.acknowledged and result.deleted_count == 1:
                  return True, {"message":"deletion successful"}
               else:
                  return False, {"message":"Deletion of Document unsuccessful"}
            else:
               return False, {"message":"Deletion of references unsuccessful"}
      return False, {"message":"Document not found"}

   def link_file_to_doc(self, linking_doc_id:ObjectId, file_doc_id:ObjectId):
      found_linking_doc, linking_doc_data = self.get_object_metadata(linking_doc_id)
      if found_linking_doc:
         linking_doc_collection = self._get_collection_from_id(linking_doc_id)
         if "files" in linking_doc_data:  #some references to files already exist
            if "object_references" in linking_doc_data["files"]:
               linking_doc_data["files"]["object_references"].append(file_doc_id)
               update_result = linking_doc_collection.update_one({"_id":linking_doc_id},{"$addToSet":{"files.object_references":file_doc_id}})
               return update_result.modified_count == 1
            else:
               return False, {"message":"files references are formatted wrongly"}
         else: #no references to files exist, create files attribute
            files_references = {
               "referenced_collection":"Files",
               "object_references": [
                  file_doc_id
               ]
            }
            update_result = linking_doc_collection.update_one({"_id":linking_doc_id},{"$set":{"files":files_references}})
            return update_result.modified_count == 1
      else:
         return False

      
   def parse_bson(self, data: dict):
      '''
      Helper function to parse the BSON data into Json format

      Parameters: 
         data: bson object from Mongodb
      Returns:
         dict: json dict of the given object
      '''
      return json.loads(dumps(data))

   
   def create_collection_if_not_exists(self,coll_name:str):
      if coll_name not in self.db.list_collection_names():
         self.db.create_collection(coll_name)

   def does_collection_exist(self, coll_name:str):
      all_collection = self.db.list_collection_names()
      if coll_name in all_collection:
         return True 
      return False

   def get_database(self):
      return self.db

   def get_database_name(self):
      return self.db_name

   ################Utility functions################

   def _extract_schema(self, json_schema:dict):
      '''
      Helper function to extract the bson schema
      Parameters: 
         collection: The collection where we want the schema from
      Returns:
         dict: a json dict of the schema
      '''
      if isinstance(json_schema, dict): # we want to check that part is a dict :D
         for attr in json_schema:
            if "description" in json_schema[attr]: json_schema[attr].pop("description")
            if "properties" in json_schema[attr]:
               self._extract_schema(json_schema[attr]["properties"])
      return json_schema

   #gives us all the object ids of the objects that are linked
   def _get_referenced_objects(self, json_object:dict, include_names:Optional["bool"] = False, include_collection:Optional["bool"] = False):
      '''
      Parameters:
         json_object: json of document from which the references should be retrieved
         include_names: includes the names of the referenced documents if True
         include_collection: includes the collection of the referenced documents if True
      Returns:
         Status: Boolean, True if successed, False if it failed
         List of referenced documents: a list of json objects with all the objectids and optionally names and collections of the object
      '''
      try:
         subdocs = []
         if json_object:
            if "_id" in json_object:
               json_object.pop("_id")
            for attr in json_object:
               if isinstance(json_object[attr],dict) and "object_references" in json_object[attr]:  #list of references
                  for reference in json_object[attr]["object_references"]:
                        found, subdoc_data = self.get_object_metadata(reference)
                        if found:
                           subdoc = {"_id":reference}
                           if include_names:
                              subdoc["name"] = subdoc_data["name"] if "name" in subdoc_data else ""
                           if include_collection:
                              subdoc["collection_name"] = json_object[attr]["referenced_collection"]
                           subdocs.append(subdoc)
               elif isinstance(json_object[attr],dict) and "object_reference" in json_object[attr]:
                  reference = json_object[attr]["object_reference"]
                  found, subdoc_data = self.get_object_metadata(reference)
                  if found:
                     subdoc = {"_id":reference}
                     if include_names:
                        subdoc["name"] = subdoc_data["name"] if "name" in subdoc_data else ""
                     if include_collection:
                        subdoc["collection_name"] =json_object[attr]["referenced_collection"]
                     subdocs.append(subdoc)
         return True, subdocs
      except Exception as e:
         return False, {"message":str(e)}

   def _oid_dicts_to_ObjectIds_rec(self, json_object:dict | list):
      if isinstance(json_object, list):                  #json_object can be a list because of the recursion
         for index, element in enumerate(json_object):
               if isinstance(element, dict) and "$oid" in element:
                     id = element["$oid"]
                     json_object[index] = ObjectId(id)
               elif isinstance(element, list) or isinstance(element, dict):  #if list or json object do a recursion
                  self._oid_dicts_to_ObjectIds_rec(element)
      else: #is a complete json object
         for attr in json_object:
            if isinstance(json_object[attr], dict) and "$oid" in json_object[attr]:
               id = json_object[attr]["$oid"]
               json_object[attr] = ObjectId(id)
            elif isinstance(json_object[attr], list) or isinstance(json_object[attr], dict):  #if list or json object do a recursion
               self._oid_dicts_to_ObjectIds_rec(json_object[attr])
      return json_object


   def _objectids_to_strings_rec(self, json_object:dict | list):   
      if isinstance(json_object, list):                  #json_object can be a list because of the recursion
         for index, element in enumerate(json_object):
               if isinstance(element, ObjectId):         #if ObjectId cast to string
                  json_object[index] = str(element)
               elif isinstance(element, list) or isinstance(element, dict):  #if list or json object do a recursion
                  self._objectids_to_strings_rec(element)
      else: #is a complete json object
         for object in json_object: 
            if isinstance(json_object[object], ObjectId):   #if ObjectId cast to string
               json_object[object] = str(json_object[object])
            elif isinstance(json_object[object], list) or isinstance(json_object[object], dict):  #if list or json object do a recursion
               self._objectids_to_strings_rec(json_object[object])
      return json_object

   def _get_collection_from_id(self, id:ObjectId):
      for collection_name in self.db.list_collection_names():
         collection = self.db.get_collection(collection_name)
         if collection.find_one({"_id":id}) != None:
            return collection
      return None

   def _delete_backreference(self, origin_id:ObjectId, referenced_id:ObjectId, referenced_doc_collection:pymongo.collection.Collection):
      """remove the backreference to origin_id from the object with referenced_id.
      Used when the reference referenced_id was deleted from the origin_id document."""
      update_result = referenced_doc_collection.update_one({"_id":referenced_id},{"$pull":{"h_backreferences":origin_id}})
      return update_result.modified_count == 1


   def _delete_all_references(self, referenced_id:ObjectId):
      """deletes all references to the referenced_id document.
      Used when the referenced_id document was deleted"""
      error = False
      deletion_doc_found, deletion_doc_metadata = self.get_object_metadata(referenced_id, show_hidden_attributes=True)
      if deletion_doc_found:
         if 'h_backreferences' in deletion_doc_metadata:   #get all documents that reference the document
            for backref_doc_id in deletion_doc_metadata['h_backreferences']:
               backref_doc_found, backref_doc_metadata = self.get_object_metadata(backref_doc_id)
               if backref_doc_found:
                  backref_doc_collection = self._get_collection_from_id(backref_doc_id)
                  deleted_reference_sucessfully = self._delete_all_references_rec(backref_doc_metadata, backref_doc_id, backref_doc_collection, referenced_id)
                  if not deleted_reference_sucessfully:
                     return False
      else:
         return False
      return True


   def _delete_all_references_rec(self, json_object:dict | list, id_of_current_document:ObjectId, collection_of_current_document:pymongo.collection.Collection, id_to_remove:ObjectId, current_document_path:Optional["str"] = ""):
      current_document_path_with_dot = current_document_path if not current_document_path else current_document_path + "."
      no_error = True
      if isinstance(json_object, list):      #json_object can be a list because of the recursion
         for index, element in enumerate(json_object):
               if isinstance(element, dict) and "object_references" in element:  #if element is list of references
                  for reference in element["object_references"]:
                     if str(reference) == str(id_to_remove):
                        update_result = collection_of_current_document.update_one({"_id":id_of_current_document},{"$pull":{current_document_path_with_dot + str(index) + ".object_references" :id_to_remove}}) #remove reference
                        no_error = no_error & (update_result == 1)  #check if only one document was modified
               elif (isinstance(element, dict)) and ("object_reference" in element) and (str(element["object_reference"]) == str(id_to_remove)):
                  update_result = collection_of_current_document.update_one({"_id":id_of_current_document},{"$pull":{current_document_path:element}}) #remove whole object reference from list
                  no_error = no_error & (update_result == 1)  #check if only one document was modified
               elif isinstance(element, list) or isinstance(element, dict):  #if list or json object do a recursion
                  recursion_result = self._delete_all_references_rec(element, id_of_current_document, collection_of_current_document, id_to_remove, current_document_path_with_dot + str(index))
                  no_error = no_error & recursion_result
      else: #is a complete json object
         for attr in json_object:
            if isinstance(json_object[attr], dict) and "object_references" in json_object[attr]:  #if element is list of references
               for reference in json_object[attr]["object_references"]:
                  if str(reference) == str(id_to_remove):
                     update_result = collection_of_current_document.update_one({"_id":id_of_current_document},{"$pull":{current_document_path_with_dot + attr + ".object_references" :id_to_remove}}) #remove reference
                     no_error = no_error | (update_result == 1)  #check if only one document was modified
            elif (isinstance(json_object[attr], dict)) and ("object_reference" in json_object[attr]) and (str(json_object[attr]["object_reference"]) == str(id_to_remove)):
               update_result = collection_of_current_document.update_one({"_id":id_of_current_document},{"$unset":{current_document_path_with_dot + attr:""}}) #remove whole object reference from list
               no_error = no_error | (update_result == 1)  #check if only one document was modified
            elif isinstance(json_object[attr], list) or isinstance(json_object[attr], dict):  #if list or json object do a recursion
               recursion_result = self._delete_all_references_rec(json_object[attr], id_of_current_document, collection_of_current_document, id_to_remove, current_document_path_with_dot + attr)
               no_error = no_error & recursion_result
      return no_error


   def _insert_backreference(self, referencing_id:ObjectId, referenced_id:ObjectId, referenced_doc_collection:pymongo.collection.Collection):
      """adds the backreference referencing_id to the referenced_id object.
      Used when the reference referenced_id was added to referencing_id."""
      update_result = referenced_doc_collection.update_one({"_id":referenced_id},{"$addToSet":{"h_backreferences":referencing_id}})
      return update_result.modified_count == 1

if __name__ == '__main__':
   pass