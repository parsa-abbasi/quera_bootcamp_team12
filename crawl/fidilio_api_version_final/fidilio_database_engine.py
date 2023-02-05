from user_pass_temp import DatabaseLocalLoginInfo
# from user_pass_temp import DatabaseServerLoginInfo
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from urllib.parse import quote_plus
import mysql.connector
from mysql.connector import Error


class DatabaseFidilioEngine:
    def __init__(self):
        self.engin = None
        self.database_login = DatabaseLocalLoginInfo()
        self.username = self.database_login.username
        self.password = self.database_login.password
        self.host = self.database_login.host
        self.port = self.database_login.port
        self.database = "Food"
        self.create_database()
        self.connection = None

    def create_database(self):
        connection = mysql.connector.connect(host=self.host,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port)
        cursor = connection.cursor()
        cursor.execute("""CREATE DATABASE
        IF
        NOT EXISTS food CHARACTER 
        SET utf8mb4 COLLATE utf8mb4_unicode_ci;""")

        cursor.close()
        connection.close()

    def get_engine(self):
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format(quote_plus(self.username),
                                                                                   quote_plus(self.password),
                                                                                   self.host,
                                                                                   self.port,
                                                                                   self.database),
                               echo=False)

        self.engin = engine
        return engine

    def get_cursor(self):
        connection = mysql.connector.connect(host=self.host,
                                             database=self.database,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port)

        return connection

