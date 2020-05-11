import os, os.path, pandas as pd
from pandas import read_sql_query
from psycopg2 import connect, sql
from utils.tools import db, config, geoconfig
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement, WKBElement
from sqlalchemy import *
from shapely.geometry import Point
import re

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
        con = db.str
        cur = con.cursor()

        self.geo_dataframe.to_sql('dataHeader', self.engine, index=False, dtype=self.header_types)
        try:
            self.cur.execute("""
            ALTER TABLE gisdb.public."dataHeader"
            ADD PRIMARY KEY ("PrimaryKey");
            """)
            con.commit()
        except Exception as e:
            print(e)
            con = db.str
            cur = con.cursor()
