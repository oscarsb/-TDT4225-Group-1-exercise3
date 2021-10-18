from DbConnector import DbConnector
import constants
from pathlib import Path
import os



class Datahandler:
    """Class for parsing Geolife data"""

    def __init__(self):
        self.handler = DBhandler()
        self.datapath = Path(constants.DATA_PATH)
        self.userpath = Path(str(self.datapath) + r"\Data")
        self.all_users = os.listdir(self.userpath)  # all users in dataset

        with open(Path(str(self.datapath) + r"\labeled_ids.txt"), 'r') as f:
            self.labeled_users = f.read().splitlines()  # all users with labels

        self.collections = ["TrackPoint", "Activity", "User"]

    def db_close_connection(self):
        self.handler.db_close_connection()
        print("Database connection closed..")


class DBhandler:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        
        self.collection_names = ["User", "Activity", "TrackPoint"]
        self.activity_id = 1
        self.trackpoint_id = 1    


def main():
    program = None
    try:
        program = Datahandler()
        # program.drop_collections()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.handler.connection.close_connection()


if __name__ == '__main__':
    main()
