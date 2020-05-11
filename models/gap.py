import os, os.path, pandas as pd
from sqlalchemy import *

class datagap:
    engine = None
    initial_dataframe = None
    # geo_dataframe = None
    datagap_types = {
        "LineKey" : VARCHAR(100),
        "RecKey" : VARCHAR(100),
        "DateModified" : DATE(),
        "FormType" : TEXT(),
        "FormDate" : DATE(),
        "Observer" : TEXT(),
        "Recorder" : TEXT(),
        "DataEntry" : TEXT(),
        "DataErrorChecking" : TEXT(),
        "Direction" : NUMERIC(),
        "Measure" : NUMERIC(),
        "LineLengthAmount" : NUMERIC(),
        "GapMin" : NUMERIC(),
        "GapData" : NUMERIC(),
        "PerennialsCanopy" : NUMERIC(),
        "AnnualGrassesCanopy" : NUMERIC(),
        "AnnualForbsCanopy" : NUMERIC(),
        "OtherCanopy" : NUMERIC(),
        "Notes" : TEXT(),
        "NoCanopyGaps" : NUMERIC(),
        "NoBasalGaps" : NUMERIC(),
        "DateLoadedInDb" : DATE(),
        "PerennialsBasal" : NUMERIC(),
        "AnnualGrassesBasal" : NUMERIC(),
        "AnnualForbsBasal" : NUMERIC(),
        "OtherBasal" : NUMERIC(),
        "PrimaryKey" : VARCHAR(100),
        "DBKey" : TEXT(),
        "SeqNo" : TEXT(),
        "RecType" : TEXT(),
        "GapStart" : NUMERIC(),
        "GapEnd" : NUMERIC(),
        "Gap" : NUMERIC(),
        "source" : TEXT(),
        "State" : TEXT(),
        "PlotKey" : TEXT()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.gap_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('dataHeader', self.engine, index=False, dtype=self.datagap_types)
