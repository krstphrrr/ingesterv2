import os, os.path, pandas as pd
from sqlalchemy import *

class geospecies:
    engine = None
    initial_dataframe = None
    geo_dataframe = None
    geospe_types = {
        "ogc_fid": INTEGER(),
        "AH_SpeciesCover": NUMERIC(),
        "DBKey": TEXT(),
        "Duration": TEXT(),
        "GlobalID": TEXT(),
        "GrowthHabit": TEXT(),
        "GrowthHabitSub": TEXT(),
        "Hgt_Species_Avg": NUMERIC(),
        "Latitude_NAD83": NUMERIC(),
        "Longitude_NAD83": NUMERIC(),
        "Noxious": TEXT(),
        "PlotID": TEXT(),
        "PrimaryKey": VARCHAR(100),
        "SG_Group": TEXT(),
        "Species": TEXT(),
        "SpeciesState": TEXT(),
        "created_date": DATE(),
        "created_user": TEXT(),
        "last_edited_date": DATE(),
        "last_edited_user": TEXT(),
        "DateLoadedInDb": DATE()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('geoSpecies', self.engine, index=False, dtype=self.geospe_types)
