import os, os.path, pandas as pd
from sqlalchemy import *

class datasoilstability:
    engine = None
    initial_dataframe = None
    # geo_dataframe = None
    datasoilstability_types = {
        "PlotKey" : VARCHAR(100),
        "RecKey" : VARCHAR(100),
        "DateModified" : DATE(),
        "FormType" : TEXT(),
        "FormDate" : DATE(),
        "LineKey" : VARCHAR(100),
        "Observer" : TEXT(),
        "Recorder" : TEXT(),
        "DataEntry" : TEXT(),
        "DataErrorChecking" : TEXT(),
        "SoilStabSubSurface" : NUMERIC(),
        "Notes" : TEXT(),
        "DateLoadedInDb" : DATE(),
        "PrimaryKey" : VARCHAR(100),
        "DBKey" : TEXT(),
        "Position" : NUMERIC(),
        "Line" : VARCHAR(50),
        "Pos" : VARCHAR(50),
        "Veg" : TEXT(),
        "Rating" : NUMERIC(),
        "Hydro" : NUMERIC(),
        "source" : TEXT()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('geoSpecies', self.engine, index=False, dtype=self.datasoilstability_types)
