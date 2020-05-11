import os, os.path, pandas as pd
from sqlalchemy import *

class dataheight:
    engine = None
    initial_dataframe = None
    # geo_dataframe = None
    dataheight_types = {
        "PrimaryKey" : VARCHAR(100),
        "DBKey" : TEXT(),
        "PointLoc" : NUMERIC(),
        "PointNbr" : NUMERIC(),
        "RecKey" : VARCHAR(100),
        "Height" : NUMERIC(),
        "Species" : TEXT(),
        "Chkbox" : NUMERIC(),
        "type" : TEXT(),
        "GrowthHabit_measured" : TEXT(),
        "LineKey" : VARCHAR(100),
        "DateModified" : DATE(),
        "FormType" : TEXT(),
        "FormDate" : DATE(),
        "Observer" : TEXT(),
        "Recorder" : TEXT(),
        "DataEntry" : TEXT(),
        "DataErrorChecking" : TEXT(),
        "Direction" : VARCHAR(100),
        "Measure" : NUMERIC(),
        "LineLengthAmount" : NUMERIC(),
        "SpacingIntervalAmount" : NUMERIC(),
        "SpacingType" : TEXT(),
        "HeightOption" : TEXT(),
        "HeightUOM" : TEXT(),
        "ShowCheckbox" : NUMERIC(),
        "CheckboxLabel" : TEXT(),
        "source" : TEXT(),
        "UOM" : TEXT()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('geoSpecies', self.engine, index=False, dtype=self.dataheight_types)
