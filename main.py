import os, os.path, pandas as pd
from pandas import read_sql_query
from psycopg2 import connect, sql
import psycopg2
from utils.tools import db, config, geoconfig
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement, WKBElement
from sqlalchemy import *
from shapely.geometry import Point
import re
from io import StringIO

import psycopg2
from tqdm import tqdm

# from models.header import header
# from models.geospecies import geoSpecies
from models.lpi import datalpi

no = ['groups', 'ng_user', 'pages', 'PrimaryKey', 'user_group_link', 'users']
for item in i.tablenames:
    if item not in no:
        i.drop_fk(item)

lpipath = r"C:\Users\kbonefont\Desktop\data\lpi_tall.csv"
lpi = datalpi(lpipath)



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
            sql.Identifier(table))
            )
            self.con.commit()
            print(table +' dropped')
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

    def main_ingest(self,
                    df: pd.DataFrame,
                    table:str,
                    connection: psycopg2.extensions.connection,
                    chunk_size:int = 10000): #default should change with million+
                cursor = connection.cursor()
                df = df.copy()

                escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
                for col in df.columns:
                    if df.dtypes[col] == 'object':
                        for v, e in escaped.items():

                            df[col] = df[col].apply(lambda x: x.replace(v, e) if isinstance(x,str) else x)
                try:
                    for i in tqdm(range(0, df.shape[0], chunk_size)):
                        f = StringIO()
                        chunk = df.iloc[i:(i + chunk_size)]

                        chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                        f.seek(0)
                        cursor.copy_from(f, table, columns=[f'"{i}"' for i in df.columns])
                        connection.commit()
                except psycopg2.Error as e:
                    print(e)
                    connection.rollback()
                cursor.close()


def createtable():
    command = """ CREATE TABLE gisdb.public."dataLPI"
    (
    "LineKey" VARCHAR(100),
    "RecKey" VARCHAR(100),
    "DateModified" DATE ,
    "FormType" TEXT ,
    "FormDate" DATE ,
    "Observer" TEXT ,
    "Recorder" TEXT ,
    "DataEntry" TEXT ,
    "DataErrorChecking" TEXT ,
    "Direction" VARCHAR(50),
    "Measure" NUMERIC ,
    "LineLengthAmount" NUMERIC ,
    "SpacingIntervalAmount" NUMERIC ,
    "SpacingType" TEXT ,
    "HeightOption" TEXT ,
    "HeightUOM" TEXT ,
    "ShowCheckbox" NUMERIC ,
    "CheckboxLabel" TEXT ,
    "PrimaryKey" VARCHAR(100),
    "DBKey" TEXT ,
    "PointLoc" NUMERIC ,
    "PointNbr" NUMERIC ,
    "ShrubShape" TEXT ,
    "layer" TEXT ,
    "code" TEXT ,
    "chckbox" INTEGER ,
    "source" TEXT ,
    "STATE" VARCHAR(50),
    "SAGEBRUSH_SPP" TEXT ,
    "PLOTKEY" VARCHAR(100)
    )
    """

    con = db.str
    cur = con.cursor()
    try:
        cur.execute(command)
        con.commit()
        # cur.execute("selec")
    except Exception as e:
        con = db.str
        cur = con.cursor()
        print(e)





































#
