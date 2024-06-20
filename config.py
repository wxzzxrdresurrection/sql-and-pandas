import mysql.connector
import pandas as pd

class DB(object):
    def __init__(self, host="", user="root", password="", database=""):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        
        self.mydb = mysql.connector.connect(
            host=self.__host,
            user= self.__user,
            password = self.__password,
            database=self.__database
        )

        if (self.mydb.is_connected()):
            self.cursor = self.mydb.cursor()
        
    def select(self, query):
        df = pd.read_sql_query(query, self.mydb)
        return df