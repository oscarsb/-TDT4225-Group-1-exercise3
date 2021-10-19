from pprint import pprint 
from DbConnector import DbConnector
import constants
from pathlib import Path
import os
from tqdm import tqdm


class DBhandler:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        
        self.collections = ["User", "TrackPoint"]
        self.activity_id = 1
        self.trackpoint_id = 1
        
        self.datapath = Path(constants.DATA_PATH)
        self.userpath = Path(str(self.datapath) + r"\Data")
        self.all_users = os.listdir(self.userpath)  # all users in dataset

    def print_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

    def drop_collections(self):
        """Drop collections"""
        for name in self.collections:
            collection = self.db[name]
            collection.drop()
            print(f"Dropped: {name}")
        
    def create_collections(self):
        """Create all collections"""
        for name in self.collections:
            self.db.create_collection(name)  
            print(f"Created: {name}")

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

    def get_users_with_labels(self):
        """Return list of users with labels boolean"""

        all_users = self.all_users.copy()  # create tmp which can be modified
        
        inserted_users = []
        
        # add labeled users to list
        for user in self.labeled_users:
            all_users.remove(user)
            inserted_users.append([user, 1]) # add users with has labels = 1

        # insert users without labels
        for user in all_users:
            inserted_users.append([user, 0])
            
        return inserted_users
        
    def insert_data(self):   
        """Insert users with activities,
        assumes User table exists"""
        users = self.get_users_with_labels()

        docs = []

        # adds user with activities when trackpoints are inserted and activities retrieved
        for user in tqdm(users, ncols=100, leave=False, desc="Inserting data from all users"):
            user_format = {
                "_id": f"{user[0]}",
                "has_labels": user[1],
                "activities": self.get_user_activities_and_insert_trackpoints(user[0])
            }
            docs.append(user_format)
            
        
        collection = self.db["User"]
        collection.insert_many(docs)
        print(f"Inserted {len(docs)} users.")

    def get_user_activities_and_insert_trackpoints(self, user):
        """Returns activities for a user"""
        activities = []

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
                
            # add all trackpoints to db
            self.insert_trackpoint_documents(track_points)

            startDatetime=track_points[0][5]
            endDatetime=track_points[-1][5]
            transportation = "NULL"

            # if file match label entry, add the transportation mode
            for label in label_formated:
                if label[0] == track_points[0][5] and label[1] == track_points[-1][5]:
                    transportation = label[2]
                    break
            
            # create activity doc
            doc = {
                "_id": f"{self.activity_id}",
                "transportation_mode": f"{transportation}",
                "start_date_time": f"{startDatetime}",
                "end_date_time": f"{endDatetime}"
            }
            self.activity_id += 1
            activities.append(doc)

        return activities
    
    def insert_trackpoint_documents(self, trackpoints):      
        """Inserts list of trackpoints, 
        assumes TrackPoint table exists"""  
        docs = []          
        
        for track in trackpoints:
            track_format = {
                "_id": self.trackpoint_id,
                "lat": track[0],
                "lon": track[1],
                "altitude": track[3],
                "date_time": f"{track[5]}",
                "activity_id": self.activity_id
            }
            docs.append(track_format)
            self.trackpoint_id += 1
            
        collection = self.db["TrackPoint"]
        collection.insert_many(docs)


def main():
    program = None
    try:
        program = DBhandler()
        #program.drop_collections()
        #program.create_collections()
        #program.insert_data()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
            print("Database connection closed..")


if __name__ == '__main__':
    main()
