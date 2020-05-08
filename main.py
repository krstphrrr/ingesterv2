import os, os.path, pandas as pd
from pandas import read_sql_query
from psycopg2 import connect, sql
from utils.tools import db, config, geoconfig
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement, WKBElement
from sqlalchemy import *
from shapely.geometry import Point
import re
headerpath = r"C:\Users\kbonefont\Desktop\data\1_header.csv"
geospepath = r"C:\Users\kbonefont\Desktop\data\geoSpecies_2.csv"

class header:
    engine = None
    initial_dataframe = None
    geo_dataframe = None
    header_types = {
        "PrimaryKey":VARCHAR(100),
        "SpeciesState": VARCHAR(2),
        "PlotID": TEXT(),
        "PlotKey": VARCHAR(50),
        "DBKey": TEXT(),
        "EcologicalSiteId": VARCHAR(50),
        "Latitude_NAD83": NUMERIC(),
        "Longitude_NAD83": NUMERIC(),
        "State": VARCHAR(2),
        "County": VARCHAR(50),
        "DateEstablished": DATE(),
        "DateLoadedInDb": DATE(),
        "ProjectName": TEXT(),
        "source": TEXT(),
        "LocationType": VARCHAR(20),
        "DateVisited": DATE(),
        "Elevation": NUMERIC(),
        "PercentCoveredByEcoSite": NUMERIC(),
        'wkb_geometry': Geometry('POINT', srid=4326)
        }
    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)
        self.gdf = gpd.GeoDataFrame(
            self.initial_dataframe,
            crs='epsg:4326',
            geometry = [
                Point(xy) for xy in zip(self.initial_dataframe.Longitude_NAD83,
                self.initial_dataframe.Latitude_NAD83)
                ]
            )
        self.geo_dataframe['wkb_geometry'] = self.geo_dataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
        self.geo_dataframe.drop('geometry', axis=1, inplace=True)

    def send_to_pg(self):

        self.geo_dataframe.to_sql('dataHeader', self.engine, index=False, dtype=self.header_types)





header = pd.read_csv(headerpath)


i = ingesterv2()

no = ['groups', 'ng_user', 'pages', 'PrimaryKey', 'user_group_link', 'users']
for item in i.tablenames:
    if item not in no:
        i.drop_fk(item)




gdf.to_sql('dataHeader', engine, index=False, dtype=header_types)



i.drop_fk('dataGap')
i._drop_fk('dataHeight')
i.drop_table('dataHeader_geo')
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














































#
