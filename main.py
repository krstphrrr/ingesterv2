import os, os.path, pandas as pd
from pandas import read_sql_query
from psycopg2 import connect, sql
from utils.tools import db, config, geoconfig
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement, WKBElement
from sqlalchemy import *
from shapely.geometry import Point
import re

from models.header import header
from models.geospecies import geoSpecies

no = ['groups', 'ng_user', 'pages', 'PrimaryKey', 'user_group_link', 'users']
for item in i.tablenames:
    if item not in no:
        i.drop_fk(item)





i = ingesterv2()
i.tablenames

i.reestablish_fk('geoSpecies')
class ingesterv2:
    # connection properties
    con = None
    cur = None
    # data pull on init
    tablenames = []
    __seen = set()


    def __init__(self):
        """ clearing old instances """
        self.con = None
        self.cur = None
        self.tablenames = []
        self.__seen = set()

        """ init connection objects """
        self.con = db.str
        self.cur = self.con.cursor()
        """ populate properties """
        self.pull_tablenames()

    def pull_tablenames(self):
        if self.con is not None:

            try:
                self.cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;""")
                query_results = self.cur.fetchall()

                for table in query_results:
                    if table not in self.__seen:
                        self.__seen.add(re.search(r"\(\'(.*?)\'\,\)",
                        str(table)).group(1))
                        self.tablenames.append(re.search(r"\(\'(.*?)\'\,\)",
                        str(table)).group(1))
            except Exception as e:
                print(e)
                self.con = db.str
                self.cursor = self.con.cursor
        else:
            print("connection object not initialized")
    def drop_fk(self, table):

        key_str = "{}_PrimaryKey_fkey".format(str(table))
        print('try: dropping keys...')
        try:
            # print(table)
            self.cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   DROP CONSTRAINT IF EXISTS {1}""").format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            self.con.commit()
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()
        print(f"Foreign keys on {table} dropped")
    def drop_table(self, table):
        try:

            self.cur.execute(
            sql.SQL("DROP TABLE IF EXISTS gisdb.public.{};").format(
            sql.Identifier(item))
            )
            self.con.commit()
            print(item +' dropped')
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()
    def reestablish_fk(self,table):
        key_str = "{}_PrimaryKey_fkey".format(str(table))

        try:

            self.cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   FOREIGN KEY ("PrimaryKey")
                   REFERENCES "dataHeader"("PrimaryKey");
                   """).format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            self.con.commit()
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()














































#
