import os, os.path, pandas as pd
from sqlalchemy import *

class geospecies:
    engine = None
    initial_dataframe = None
    geo_dataframe = None
    checked_df = None
    sqlalchemy_types = {
        "ogc_fid" : INTEGER(),
        "AH_SpeciesCover" : NUMERIC(),
        "DBKey" : TEXT(),
        "Duration" : TEXT(),
        "GlobalID" : TEXT(),
        "GrowthHabit" : TEXT(),
        "GrowthHabitSub" : TEXT(),
        "Hgt_Species_Avg" : NUMERIC(),
        "Latitude_NAD83" : NUMERIC(),
        "Longitude_NAD83" : NUMERIC(),
        "Noxious" : TEXT(),
        "PlotID" : TEXT(),
        "PrimaryKey" : VARCHAR(100),
        "SG_Group" : TEXT(),
        "Species" : TEXT(),
        "SpeciesState" : TEXT(),
        "created_date" : DATE(),
        "created_user" : TEXT(),
        "last_edited_date" : DATE(),
        "last_edited_user" : TEXT(),
        "DateLoadedInDb" : DATE()
        }
    pandas_dtypes = {
        "ogc_fid" : "Int64",
        "AH_SpeciesCover" : "float64",
        "DBKey" : "object",
        "Duration" : "object",
        "GlobalID" : "object",
        "GrowthHabit" : "object",
        "GrowthHabitSub" : "object",
        "Hgt_Species_Avg" : "float64",
        "Latitude_NAD83" : "float64",
        "Longitude_NAD83" : "float64",
        "Noxious" : "object",
        "PlotID" : "object",
        "PrimaryKey" : "object",
        "SG_Group" : "object",
        "Species" : "object",
        "SpeciesState" : "object",
        "created_date" : "datetime64[ns]",
        "created_user" : "object",
        "last_edited_date" : "datetime64[ns]",
        "last_edited_user" : "object",
        "DateLoadedInDb" : "datetime64[ns]"
    }
    psycopg2_command = """ CREATE TABLE gisdb.public."dataLPI"
        (
        "ogc_fid" INTEGER ,
        "AH_SpeciesCover" NUMERIC ,
        "DBKey" TEXT ,
        "Duration" TEXT ,
        "GlobalID" TEXT ,
        "GrowthHabit" TEXT ,
        "GrowthHabitSub" TEXT ,
        "Hgt_Species_Avg" NUMERIC ,
        "Latitude_NAD83" NUMERIC ,
        "Longitude_NAD83" NUMERIC ,
        "Noxious" TEXT ,
        "PlotID" TEXT ,
        "PrimaryKey" VARCHAR(100),
        "SG_Group" TEXT ,
        "Species" TEXT ,
        "SpeciesState" TEXT ,
        "created_date" DATE ,
        "created_user" TEXT ,
        "last_edited_date" DATE ,
        "last_edited_user" TEXT ,
        "DateLoadedInDb" DATE
        )
        """

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)

        """ fieldtype check """
        for i in self.checked_df.columns:
            if self.checked_df[i].dtype!=self.datalpi_dtypes[i]:
                self.checked_df[i] = self.typecast(df=self.checked_df,field=i,fieldtype=self.pandas_dtypes[i])


    def send_to_pg(self):

        self.initial_dataframe.to_sql('geoSpecies', self.engine, index=False, dtype=self.geospe_types)
        
    def create_empty_table(self):
        con = db.str
        cur = con.cursor()
        try:
            cur.execute(self.psycopg2_command)
            con.commit()
            # cur.execute("selec")
        except Exception as e:
            con = db.str
            cur = con.cursor()
            print(e)
