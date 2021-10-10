from pprint import pprint 
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

    def create_collections(self):
        """Creates User, Activity and TrackPoint collection"""
        self.handler.create_collections()

    def drop_collections(self):
        """Drop collections"""
        self.handler.drop_collections()

    def insert_users(self):
        """Insert all user into User collection,
        assumes User table exists"""

        all_users = self.all_users.copy()  # create tmp which can be modified
        
        inserted_users = []
        
        # add labeled users to list
        for user in self.labeled_users:
            all_users.remove(user)
            inserted_users.append([user, 1]) # add users with has labels = 1

        # insert users without labels
        for user in all_users:
            inserted_users.append([user, 0])
            
        self.handler.insert_user_documents(inserted_users)

    def _format_labels(self, user, user_dir):
        """Read label file and format data"""
        label_formated = []  # all labels, format: ["starttime","endtime", "transportMode"]
        if user in self.labeled_users:
            # only if user is labeled user

            with open(Path(str(user_dir) + r"\labels.txt"), 'r') as f:
                raw_label = f.read().splitlines()[1:]

            for line in raw_label:
                # format all label data to match dataset
                list_line = list(line)  # turn str into list
                list_line[10] = "_"
                list_line[30] = "_"

                line = "".join(list_line)  # add changes
                line = line.split()

                tmp_label = []
                # replace characters with correct symbols
                for i in line:
                    val = i.replace("/", "-")
                    val = val.replace("_", " ")
                    tmp_label.append(val)

                label_formated.append(tmp_label)  # add formated label data

        return label_formated

    def insert_activities_and_trackpoints(self):
        """"Function for adding Activities and matching trackPoints"""

        for user in self.all_users:
            # data directory of given user
            user_dir = Path(str(self.userpath) + rf"\{user}")

            # all activity files of given user
            traj_path = Path(str(user_dir) + r"\Trajectory")
            files = os.listdir(traj_path)

            label_formated = self._format_labels(user, user_dir)

            for file in files:
                with open(Path(str(traj_path) + rf"\{file}")) as f:
                    track_raw = f.read().splitlines()[6:]

                # if activity is to large, ignore it
                if len(track_raw) > 2500:
                    continue

                track_points = []
                # format each trackPoint and add all values to list
                for line in track_raw:
                    formated = line.replace(",", " ").split()
                    # change date and time format to match datetime
                    formated[5] = f"{formated[5]} {formated[6]}"
                    formated.remove(formated[-1])  # remove time

                    track_points.append(formated)

                transportation = "NULL"

                # if file match label entry, add the transportation mode
                for label in label_formated:
                    if label[0] == track_points[0][5] and label[1] == track_points[-1][5]:
                        transportation = label[2]
                        break

                self.handler.insert_activity_document(user_id=user, transportationMode=transportation, startDatetime=track_points[0][5],
                    endDatetime=track_points[-1][5])
                
                # add all trackpoints to db
                self.handler.insert_trackpoint_documents(track_points)

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
        
        
    def create_collections(self):
        """Create all collections"""
        for name in self.collection_names:
            self.db.create_collection(name)  
            print(f"Created: {name}")
        
    def insert_user_documents(self, users):   
        """Add list of users"""     
        docs = []  
        for user in users:
            user_format = {
                "_id": f"{user[0]}",
                "has_labels": user[1],
                # TODO: add activity object list?
            }
            docs.append(user_format)
        
        collection = self.db["User"]
        collection.insert_many(docs)
        print(f"inserted {len(docs)} users.")
        
    def insert_activity_document(self,user_id,
                                  transportationMode,
                                  startDatetime,
                                  endDatetime):
        """Add list of activities"""
        # TODO insert multiple documents at once?        
        doc = {
                "_id": f"{self.activity_id}",
                "transportation_mode": f"{transportationMode}",
                "start_date_time": f"{startDatetime}",
                "end_date_time": "{endDatetime}",
                "user_id" : "{user_id}" # foreign key
            }
        self.activity_id += 1
        collection = self.db["Activity"]
        collection.insert_one(doc)
    
    def insert_trackpoint_documents(self, trackpoints):      
        """Add list of trackpoints"""  
        docs = []          
        
        for track in trackpoints:
            track_format = {
                "_id": self.trackpoint_id,
                "lat": track[0],
                "lon": track[1],
                "altitude": track[3],
                "date_time": f"{track[5]}",
                "activity_id": self.activity_id - 1 # trackpoint of previous id
            }
            docs.append(track_format)
            self.trackpoint_id += 1
            
        collection = self.db["TrackPoint"]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        
    def drop_collections(self):
        for name in self.collection_names:
            collection = self.db[name]
            collection.drop()
            print(f"Dropped: {name}")
        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = Datahandler()
        # program.drop_collections()
        # program.create_collections()
        # program.insert_users()
        # program.insert_activities_and_trackpoints()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.handler.connection.close_connection()


if __name__ == '__main__':
    main()
