import os, os.path, pandas as pd
from sqlalchemy import *

class datalpi:
    engine = None
    initial_dataframe = None
    # geo_dataframe = None
    datalpi_types = {
        "LineKey" : VARCHAR(100),
        "RecKey" : VARCHAR(100),
        "DateModified" : DATE(),
        "FormType" : TEXT(),
        "FormDate" : DATE(),
        "Observer" : TEXT(),
        "Recorder" : TEXT(),
        "DataEntry" : TEXT(),
        "DataErrorChecking" : TEXT(),
        "Direction" : VARCHAR(50),
        "Measure" : NUMERIC(),
        "LineLengthAmount" : NUMERIC(),
        "SpacingIntervalAmount" : NUMERIC(),
        "SpacingType" : TEXT(),
        "HeightOption" : TEXT(),
        "HeightUOM" : TEXT(),
        "ShowCheckbox" : NUMERIC(),
        "CheckboxLabel" : TEXT(),
        "PrimaryKey" : VARCHAR(100),
        "DBKey" : TEXT(),
        "PointLoc" : NUMERIC(),
        "PointNbr" : NUMERIC(),
        "ShrubShape" : TEXT(),
        "layer" : TEXT(),
        "code" : TEXT(),
        "chckbox" : INTEGER(),
        "source" : TEXT()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.gap_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('dataHeader', self.engine, index=False, dtype=self.datalpi_types)
