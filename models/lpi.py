import os, os.path, pandas as pd
from sqlalchemy import *

class datalpi:
    engine = None
    initial_dataframe = None
    checked_df = None
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
        "source" : TEXT(),
        "STATE" : VARCHAR(50),
        "SAGEBRUSH_SPP": TEXT(),
        "PLOTKEY":VARCHAR(100)
        }
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
            "chckbox" : "Int64",
            "source" : "object",
            "STATE" : "object",
            "SAGEBRUSH_SPP": "object",
            "PLOTKEY":"object"
            }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.gap_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path, low_memory=False)
        self.checked_df = self.initial_dataframe.copy()

        """ fieldtype check """
        for i in self.checked_df.columns:
            if self.checked_df[i].dtype!=self.datalpi_dtypes[i]:
                self.checked_df[i] = self.typecast(df=self.checked_df,field=i,fieldtype=self.datalpi_dtypes[i])

    def typecast(self,df,field,fieldtype):
        data = df
        castfield = data[field].astype(fieldtype)
        # if 'chckbox' in field:
        #     data[field] = data[field].apply()
        return castfield

    def send_to_pg(self):

        self.initial_dataframe.to_sql('dataLPI', self.engine, index=False, dtype=self.datalpi_types)
