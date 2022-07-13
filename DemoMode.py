from DatabaseHandler import DatabaseHandler
from FileServerHandler import FileServerHandler    
    

class DemoMode():
    database_handler : DatabaseHandler = None

    def __init__(self,db_handler:DatabaseHandler):
        self.database_handler = db_handler

    def demo_mode(self):
        to_insert = [
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041036"},"name": "Gear_01","size": "20", "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041143"}}}],
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041037"},"name": "Gear_02","size": "30", "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041144"}}}],
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041038"},"name": "Gear_03","size": "10", "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041145"}}}],
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041039"},"name": "Cylinder","size": "10", "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041145"}}}],
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041040"},"name": "Engine","parts":{"referenced_collection": "Parts","object_references": [{"$oid": "62c7f55fed3f2993ac041036"},{"$oid": "62c7f55fed3f2993ac041037"},{"$oid":"62c7f55fed3f2993ac041038"}]}, "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041145"}}}],
            ["Parts2", {"_id": {"$oid": "62c7f55fed3f2993ac041041"},"name": "Window","Tinted": "False", "assinged_worker":{"referenced_collection": "Employees","object_reference": {"$oid": "62c7f55fed3f2993ac041045"}}}],
            ["Products", {"name": "Car","parts":{"referenced_collection": "Parts","object_references": [{"$oid": "62c7f55fed3f2993ac041040"},{"$oid": "62c7f55fed3f2993ac041041"}]}}], 
            ["Employees", {"_id": {"$oid": "62c7f55fed3f2993ac041143"},"name": "Joe","employement Start": "20.01.2012"}],
            ["Employees", {"_id": {"$oid": "62c7f55fed3f2993ac041144"},"name": "Bob","employement Start": "04.01.2015"}],
            ["Employees", {"_id": {"$oid": "62c7f55fed3f2993ac041145"},"name": "Michael","employement Start": "05.01.2020"}]
            ]
        #if self.database_handler.get_all_collections()[1]["collections"] == []:
        for collection in self.database_handler.get_database().list_collection_names():
            self.database_handler.get_database().drop_collection(collection)
        for docs in to_insert:
            self.database_handler.create_collection_if_not_exists(docs[0])
            self.database_handler.insert_object(docs[0],docs[1])
