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
lpi.checked_df['chckbox'].unique()
lpi.initial_dataframe.loc["PrimaryKey"]
lpi.initial_dataframe
for i in lpi.initial_dataframe.columns:
    print(i, lpi.initial_dataframe[i].dtype)
import numpy as np
lpi.initial_dataframe['chckbox'].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype("Int64")
np.nan==lpi.initial_dataframe['chckbox'][12509196]
pd.NA
lpi.initial_dataframe["chckbox"].astype("int64")
lpi.send_to_pg()
i = ingesterv2()
i.tablenames
lpi.initial_dataframe["PrimaryKey"][0]
i.drop_table('dataLPI')
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



df = pd.DataFrame({'float': [1.0],
                   'int': [1],
                   'datetime': [pd.Timestamp('20180310')],
                   'string': ['foo']})

df.datetime.dtype!=='datetime64[ns]'
test.DateModified.astype('datetime64[ns]')
test = lpi.initial_dataframe[:5].copy(deep=True)
for i in test.columns:
    if test[i].dtype!=datalpi_dtypes[i]:
        test[i] = typecast(test,i,datalpi_dtypes[i])
test["PointNbr"].unique()
[i for i in test.columns]
lpi.initial_dataframe["PointLoc"].unique()
lpi.initial_dataframe["chckbox"].unique()
lpi.initial_dataframe.iloc[:30,:]
datalpi_dtypes = {
        "LineKey" : "object",
        "RecKey" : "object",
        "DateModified" : "datetime64[ns]",
        "FormType" : "object",
        "FormDate" : "object",
        "Observer" : "object",
        "Recorder" : "object",
        "DataEntry" : "object",
        "DataErrorChecking" : "object",
        "Direction" : "object",
        "Measure" : "float64",
        "LineLengthAmount" : "float64",
        "SpacingIntervalAmount" : "float64",
        "SpacingType" : "object",
        "HeightOption" : "object",
        "HeightUOM" : "object",
        "ShowCheckbox" : "float64",
        "CheckboxLabel" : "object",
        "PrimaryKey" : "object",
        "DBKey" : "object",
        "PointLoc" : "float64",
        "PointNbr" : "float64",
        "ShrubShape" : "object",
        "layer" : "object",
        "code" : "object",
        "chckbox" : "int64",
        "source" : "object",
        "STATE" : "object",
        "SAGEBRUSH_SPP": "object",
        "PLOTKEY":"object"
        }
def typecast(df,field,fieldtype):
    data = df
    castfield = data[field].astype(fieldtype)
    return castfield
typecast(test, 'chckbox', datalpi_dtypes['chckbox'])
test.iloc[:,:15]
test.iloc[:,15:]
def copy_from(df: pd.DataFrame,
              table: str,
              connection: psycopg2.extensions.connection,
              chunk_size: int = 10000):
    cursor = connection.cursor()
    df = df.copy()

    # escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
    # for col in df.columns:
    #     if df.dtypes[col] == 'object':
    #
    #         for v, e in escaped.items():
    #             df[col] = df[col].str.replace(v, e)
    try:
        for i in tqdm(range(0, df.shape[0], chunk_size)):
            f = StringIO()
            chunk = df.iloc[i:(i + chunk_size)]

            chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='default', quoting=None)
            f.seek(0)
            cursor.copy_from(f, table, columns=[f'"{i}"' for i in df.columns])
            connection.commit()
    except psycopg2.Error as e:
        print(e)
        connection.rollback()
    cursor.close()
escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
testdf = lpi.checked_df.copy()
for col in testdf.columns:
    if testdf.dtypes[col] == 'object':
        for v, e in escaped.items():
            testdf[col] = testdf["LineKey"].str.replace(v, e)
lpi.checked_df.iloc[3134:3137,:].to_csv(StringIO(), index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
[lpi.checked_df.iloc[3136:3137,i] for i in range(0,len(lpi.checked_df.columns))]
lpi.checked_df.iloc[0,2]
copy_from(lpi.checked_df, 'gisdb.public."dataLPI"', db.str, 10000)
db.str










































#
