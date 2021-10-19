from pprint import pprint 
from DbConnector import DbConnector
from tabulate import tabulate


class DBhandler:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db 

    def print_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

    def get_num(self, table_name):
        return []

    def get_avg_activities_for_user(self):
        return 0

    def get_max_activities_for_user(self):
        return []

    def get_min_activities_for_user(self):
        return []

    def get_top_10_users_with_most_activities(self):
        # might be useful as starting point
        result = self.db["User"].aggregate([
            { 
                "$project": { "_id":1, "activities": { "$size": "$activities"}}
            }
        ])
        return []
        
    def ended_activity_at_the_next_day(self):
        return []

    def get_same_activities(self):
        return []

    def get_number_of_close_users(self):
        return []

    def find_users_with_no_taxi(self):
        userCollection = self.db["User"]
        result = userCollection.find(
            # find users with no activities using taxi
            { 
                "activities.transportation_mode": { "$not": { "$regex": "taxi" }}
            }
        )
        res = []
        for r in result:
            res.append(r["_id"])
        res.sort()
        return res

    def count_users_per_transport_mode(self):
        userCollection = self.db["User"]
        result = userCollection.aggregate([
            # "merge" user and activities
            {
                "$unwind": "$activities"
            },
            # ignore activities with no tranportation mode
            { 
                "$match": {
                    "activities.transportation_mode": { "$not": { "$regex": "NULL" }}
                }
            },
            # group transportation mode and user id to get 
            {
                "$group": {
                    "_id": {
                        "transport": "$activities.transportation_mode", 
                        "user_id": "$_id"
                    }
                }
            },
            # group on transportation mode and count user ids
            {
                "$group": {
                    "_id": "$_id.transport",
                    "count": { "$sum": 1 }
                }
            }
        ])
        res = []
        for r in result:
            res.append((r["_id"], r["count"]))
        return res

    def find_date_with_most_activities(self):
        return []

    def find_user_with_most_activities(self):
        return []

    def find_distance_walked_in_year_by_user(self, year, user_id):
        return []

    def find_20_users_with_most_altitude_gain(self):
        return []

    def find_all_users_with_invalid_activities(self):
        return []
    

def main():
    program = None
    try:
        program = DBhandler()
        #program.print_documents("User")

        """ 1. How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database). """
        #print("Number of users: ", program.get_num("User"))
        #print("Number of activities: ", program.get_num("Activity"))
        #print("Number of trackpoints: ", program.get_num("TrackPoint"))

        """ 2. Find the average, minimum and maximum number of activities per user. """
        avg_activity_for_all_users = program.get_avg_activities_for_user()
        #print("Avrage activities for all users: ", avg_activity_for_all_users, "≈", round(avg_activity_for_all_users, 1))
        #print("Maximum number of activities: ", program.get_max_activities_for_user())
        #print("Minimum number of activities: ", program.get_min_activities_for_user())

        """ 3. Find the top 10 users with the highest number of activities. """
        #print("TOP 10 users with the highest number of activities:")
        #pprint(program.get_top_10_users_with_most_activities())

        """ 4. Find the number of users that have started the activity in one day and ended
        the activity the next day. """
        #print("Number of users that have an activity that ended the day after it started: ", program.ended_activity_at_the_next_day())

        """ 5. Find activities that are registered multiple times. You should find the query
        even if you get zero results. """
        #pprint(program.get_same_activities())

        """ 6. An infected person has been at position (lat, lon) (39.97548, 116.33031) at
        time ‘2008-08-24 15:38:00’. Find the user_id(s) which have been close to this
        person in time and space (pandemic tracking). Close is defined as the same
        minute (60 seconds) and space (100 meters). (This is a simplification of the
        “unsolvable” problem given i exercise 2). """
        #print("The number of users which have been close to each other:")
        #pprint(program.get_number_of_close_users())

        """ 7. Find all users that have never taken a taxi. """
        #print("Users that have never taken a taxi:")
        #print(program.find_users_with_no_taxi())

        """ 8. Find all types of transportation modes and count how many distinct users that 
        have used the different transportation modes. Do not count the rows where the
        transportation mode is null. """
        #print("Number of distinct users that have used the different transportation modes:")
        #print(tabulate(program.count_users_per_transport_mode()))

        """ 9. a) Find the year and month with the most activities. """
        #print("The year and month with the most activities:")
        #pprint(program.find_date_with_most_activities())

        """ 9. b) Which user had the most activities this year and month, and how many
        recorded hours do they have? Do they have more hours recorded than the user
        with the second most activities? """
        #print("The two users with the most activities that year and month:")
        #pprint(program.find_user_with_most_activities())

        """ 10. Find the total distance (in km) walked in 2008, by user with id=112. """
        #print("Total distance walked by user 112 in 2008:")
        #pprint(program.find_distance_walked_in_year_by_user(2008, 112), "km")

        """ 11. Find the top 20 users who have gained the most altitude meters.
            Output should be a table with (id, total meters gained per user).
            Remember that some altitude-values are invalid
            Tip: (tpn.altitude-tpn-1.altitude), tpn.altitude >tpn-1.altitude """
        #print("Top 20 users who have gained the most altitude meters:")
        #pprint(program.find_20_users_with_most_altitude_gain())

        """ 12. Find all users who have invalid activities, and the number of invalid activities 
        per user. An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes. """
        #print("Users with invalid activities: ")
        #pprint(program.find_all_users_with_invalid_activities())

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
