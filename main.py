import os
from bson.objectid import ObjectId
from flask import Flask, request, send_file
from flask_restx import Api, Resource
from flask_cors import CORS

from DatabaseHandler import DatabaseHandler
from FileServerHandler import FileServerHandler
from ValidationHandler import ValidationHandler
from DemoMode import DemoMode

########################################################################

app = Flask(__name__)
api = Api(app,title="MongoDB Managment API", description="API for working with a MongoDB for data produced during machine part production")
CORS(app)

if(os.environ.get('SERVER') == "true"):
   database_handler = DatabaseHandler(os.environ.get('MONGODB_CONNSTRING') + "/?retryWrites=true&w=majority")
else:
   database_handler = DatabaseHandler("mongodb+srv://SPPTeamData:P3stTAlecCwf3vas@wzltest.hyice.mongodb.net/?retryWrites=true&w=majority")

if(os.environ.get('DEMO') == "true"):
   demo_mode = DemoMode(database_handler)

file_handler = FileServerHandler(database_handler)

validation_handler = ValidationHandler(database_handler,file_handler)

ALLOWED_EXTENSIONS = {'txt', 'json', 'pdf'}

@api.route("/collections/all")
class Collection(Resource):

   def get(self):
      '''Get a list of all collections in the database'''
      status, returned_value = database_handler.get_all_collections()
      if status:
         return database_handler.parse_bson(returned_value)
      else:
         return returned_value, 500

@api.route("/collections/validate")
class Validate(Resource):

   def get(self):
      '''Validate all collection and return violating document ids'''
      returned_value = database_handler._objectids_to_strings_rec(validation_handler.get_schema_violating_documents())
      return returned_value

@api.route("/collections/nullObjects")
class NullObjects(Resource):

   def get(self):
      '''Validate all collection and return violating document ids'''
      returned_value = validation_handler.get_documents_with_null_fields()
      return database_handler._objectids_to_strings_rec(returned_value)

@api.route("/collections/tree/<string:coll_name>")
@api.doc(params={"coll_name":"Name of the collection for which the documents should be returned"})
class CollectionTree(Resource):

   def get(self, coll_name):
      '''Get a list of all documents in the collection. Returns id and name attributes for each object'''
      status, doc_list = database_handler.get_root_tree(coll_name)
      if status:
         return database_handler._objectids_to_strings_rec(doc_list)
      else:
         return doc_list, 500


@api.route("/collections/schema/<string:coll_name>")
class Schema(Resource):

   @api.doc(params={"coll_name":"Name of the collection for which the schema should be returned"})
   def get(self, coll_name):
      '''Get the required attributes and their type for objects in the collection'''
      schema = validation_handler.get_schema(coll_name)
      if schema:  #if schema is not empty
         return schema  #don't wrap in parse_bson because it will destroy the schema
      else:
         return {"message": "No schema found for collection or collection does not exist"}, 500  


@api.route("/collections/hints/<string:coll_name>")
class Hints(Resource):

   @api.doc(params={"coll_name":"Name of the collection for which the hints should be returned"})
   def get(self, coll_name):
      '''Get the required attributes and their description for objects in the collection'''
      return {"message":"not implemented"}, 500


@api.route("/collections/validate")
class Validate(Resource):

   def get(self):
      '''Validate all collection and return violating document ids'''
      returned_value = validation_handler.get_schema_violating_documents()
      return database_handler._objectids_to_strings_rec(returned_value)

@api.route("/collections/nullObjects")
class NullObjects(Resource):

   def get(self):
      '''Validate all collection and return violating document ids'''
      returned_value = validation_handler.get_documents_with_null_fields()
      return database_handler._objectids_to_strings_rec(returned_value)

      
@api.route("/object/subobjects/<string:id>")
class Subobjects(Resource):

   @api.doc(params={"id":"id of the object for which the linked objects should be returned"})
   def get(self, id):
      '''Get a list ob objects that the current object links to in the database'''

      status, returned_value = database_handler.get_subobjects(ObjectId(id))
      
      if status:
         return database_handler.parse_bson(returned_value)
      else:
         return returned_value, 500
     
      
@api.route("/object/attributes/<string:id>")
class DocumentAttributes(Resource):
   
   @api.doc(params={"id":"id of the object for which the attributes should be returned"})
   def get(self, id):
      '''Get the attributes of the document with the id as json'''
      status, returned_value = database_handler.get_object_metadata(ObjectId(id))

      if status:
         return database_handler.parse_bson(returned_value)
      else:
         return returned_value, 500  
      
   @api.doc(params={"id":"id of the object which's attributes should be overwritten"})
   def put(self, id):
      '''Overwrite the attributes of the object with id with the json data specified in the request body.'''
      json_data = request.get_json()
      overwrite_successfull, message = database_handler.overwrite_object_metadata(ObjectId(id),json_data)
      if overwrite_successfull:
         return message
      else:
         return message, 500 #TODO: Implement more error codes/detection

@api.route('/object/ReferenceAttributeNames/<string:doc_id>')
class ReferenceAttributeNames(Resource):

   def get(self, doc_id):
      found_doc, output = database_handler.get_refrence_attributes_name(ObjectId(doc_id))
      return output


@api.route('/object/<string:id>',methods=['DELETE'])
@api.route('/object/new/<string:coll_name>',methods=['POST'])
class Document(Resource):
   
   @api.doc(params={"coll_name":"Name of the collection in which the object should be inserted"})
   def post(self, coll_name):
      '''Insert object into collection coll_name and returns the id if successfull and an error messag otherwise'''
      json_data = request.get_json()
      success, id = database_handler.insert_object(coll_name, json_data)
      if success:
         return database_handler._objectids_to_strings_rec(id)
      else:
         return id, 500 #TODO: Implement more error codes/detection

   @api.doc(params={"id":"id of the object which should be deleted"})
   def delete(self, id):
      '''Delete the object with id'''
      if database_handler.delete_document(ObjectId(id)):
         return {"message":"deleted sucessfully"}
      else:
         return {"message":"deletion failed"}


@api.route("/file/upload", methods=['POST'])
@api.route("/file/download/<string:file_doc_id>", methods=['GET'])
class FileUpload(Resource):
   
   def allowed_file(self, filename):
      return '.' in filename and filename.rsplit('.', 1)[-1].lower() in ALLOWED_EXTENSIONS

   def post(self):
      '''Save the included file on the server'''
      # check if the post request has the file part
      if 'file' not in request.files:
         return {"message":"No file part"}, 400
      file = request.files['file']
      # If the user does not select a file, the browser submits an empty file without a filename.
      if file.filename == '':
         return {"message":"No file send"}, 400

      if file and self.allowed_file(file.filename):
         file_saved, file_doc_id = file_handler.save(file)
         if file_saved:
            return database_handler._objectids_to_strings_rec(file_doc_id)
      return {"message":"empty file or wrong file type"}, 400

   def get(self,file_doc_id:str):
      found_file_doc, file_doc_data = database_handler.get_object_metadata(ObjectId(file_doc_id))
      if found_file_doc:
         if "filepath" in file_doc_data:
            return send_file(file_doc_data["filepath"],as_attachment=True)
         else:
            {"message":"filepath attribute missing in file document"}, 500
      else:
         return {"message":"File document no found in database"}, 500

@api.route("/file/uploadAndLink/<string:linking_doc_id>", methods=['POST'])
class FileUploadAndLink(Resource):

   def allowed_file(self, filename):
      return '.' in filename and filename.rsplit('.', 1)[-1].lower() in ALLOWED_EXTENSIONS

   def post(self,linking_doc_id:str):
      '''Save the included file on the server'''
      # check if the post request has the file part
      if 'file' not in request.files:
         return {"message":"No file part"}, 400
      file = request.files['file']
      # If the user does not select a file, the browser submits an empty file without a filename.
      if file.filename == '':
         return {"message":"No file sent"}, 400

      if file and self.allowed_file(file.filename):
         file_saved, file_doc_id = file_handler.save(file)
         if file_saved:
            if database_handler.link_file_to_doc(ObjectId(linking_doc_id),ObjectId(file_doc_id["_id"])):
               return {"message":"File saved and linked successfully"}
            else:
               return {"message":"File document created but could not be linked"}, 500
      return {"message":"empty file or wrong file type"}, 400


if __name__ == '__main__':
   #print(file_handler.get_collection_names_from_folder())
   #print(file_handler.read_schema_from_file("test"))
   #ver_handler = VerificationHandler(database_handler, file_handler)
   #ver_handler.insert_schema("VerificationTest", {"$jsonSchema": {"bsonType": 'object',"required": ['name','year']}})
   app.run(host='0.0.0.0',port='5000',debug=True)
