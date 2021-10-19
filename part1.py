from pprint import pprint 
from DbConnector import DbConnector


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


def main():
    program = None
    try:
        program = DBhandler()

        """ 1. How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database). """

        """ 2. Find the average, minimum and maximum number of activities per user. """

        """ 3. Find the top 10 users with the highest number of activities. """

        """ 4. Find the number of users that have started the activity in one day and ended
        the activity the next day. """

        """ 5. Find activities that are registered multiple times. You should find the query
        even if you get zero results. """

        """ 6. An infected person has been at position (lat, lon) (39.97548, 116.33031) at
        time ‘2008-08-24 15:38:00’. Find the user_id(s) which have been close to this
        person in time and space (pandemic tracking). Close is defined as the same
        minute (60 seconds) and space (100 meters). (This is a simplification of the
        “unsolvable” problem given i exercise 2). """

        """ 7. Find all users that have never taken a taxi. """

        """ 8. Find all types of transportation modes and count how many distinct users that 
        have used the different transportation modes. Do not count the rows where the
        transportation mode is null. """

        """ 9. a) Find the year and month with the most activities. """

        """ 9. b) Which user had the most activities this year and month, and how many
        recorded hours do they have? Do they have more hours recorded than the user
        with the second most activities? """

        """ 10. Find the total distance (in km) walked in 2008, by user with id=112. """

        """ 11. Find the top 20 users who have gained the most altitude meters.
            Output should be a table with (id, total meters gained per user).
            Remember that some altitude-values are invalid
            Tip: (tpn.altitude-tpn-1.altitude), tpn.altitude >tpn-1.altitude """

        """ 12. Find all users who have invalid activities, and the number of invalid activities 
        per user. """

        """ 1. An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes. """
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
