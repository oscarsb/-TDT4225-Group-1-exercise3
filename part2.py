from pprint import pprint 
from DbConnector import DbConnector


class Datahandler:
    """Class for parsing Geolife data"""

    def __init__(self):
        self.handler = DBhandler()

    def db_close_connection(self):
        self.handler.db_close_connection()
        print("Database connection closed..")

    def print_documents(self, collection_name):
        self.handler.fetch_documents(collection_name)
        print()


class DBhandler:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db 

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)


def main():
    program = None
    try:
        program = Datahandler()
        program.print_documents("Activity")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.handler.connection.close_connection()


if __name__ == '__main__':
    main()
