"datetime64[ns]"import os, os.path, pandas as pd
from sqlalchemy import *
from utils.tools import db

class datagap:
    engine = None
    initial_dataframe = None
    checked_df = None
    sqlalchemy_types = {
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
    datalpi_dtypes = {
        "LineKey" : "object",
        "RecKey" : "object",
        "DateModified" : "datetime64[ns]",
        "FormType" : "object",
        "FormDate" : "datetime64[ns]",
        "Observer" : "object",
        "Recorder" : "object",
        "DataEntry" : "object",
        "DataErrorChecking" : "object",
        "Direction" : "float64",
        "Measure" : "float64",
        "LineLengthAmount" : "float64",
        "GapMin" : "float64",
        "GapData" : "float64",
        "PerennialsCanopy" : "float64",
        "AnnualGrassesCanopy" : "float64",
        "AnnualForbsCanopy" : "float64",
        "OtherCanopy" : "float64",
        "Notes" : "object",
        "NoCanopyGaps" : "float64",
        "NoBasalGaps" : "float64",
        "DateLoadedInDb" : "object",
        "PerennialsBasal" : "float64",
        "AnnualGrassesBasal" : "float64",
        "AnnualForbsBasal" : "float64",
        "OtherBasal" : "float64",
        "PrimaryKey" : "object",
        "DBKey" : "object",
        "SeqNo" : "object",
        "RecType" : "object",
        "GapStart" : "float64",
        "GapEnd" : "float64",
        "Gap" : "float64",
        "source" : "object",
        "State" : "object",
        "PlotKey" : "object"
        }

    psycopg2_command = """ CREATE TABLE gisdb.public."dataLPI"
        (
        "LineKey" VARCHAR(100),
        "RecKey" VARCHAR(100),
        "DateModified" DATE ,
        "FormType" TEXT ,
        "FormDate" DATE ,
        "Observer" TEXT ,
        "Recorder" TEXT ,
        "DataEntry" TEXT ,
        "DataErrorChecking" TEXT ,
        "Direction" NUMERIC ,
        "Measure" NUMERIC ,
        "LineLengthAmount" NUMERIC ,
        "GapMin" NUMERIC ,
        "GapData" NUMERIC ,
        "PerennialsCanopy" NUMERIC ,
        "AnnualGrassesCanopy" NUMERIC ,
        "AnnualForbsCanopy" NUMERIC ,
        "OtherCanopy" NUMERIC ,
        "Notes" TEXT ,
        "NoCanopyGaps" NUMERIC ,
        "NoBasalGaps" NUMERIC ,
        "DateLoadedInDb" DATE ,
        "PerennialsBasal" NUMERIC ,
        "AnnualGrassesBasal" NUMERIC ,
        "AnnualForbsBasal" NUMERIC ,
        "OtherBasal" NUMERIC ,
        "PrimaryKey" VARCHAR(100),
        "DBKey" TEXT ,
        "SeqNo" TEXT ,
        "RecType" TEXT ,
        "GapStart" NUMERIC ,
        "GapEnd" NUMERIC ,
        "Gap" NUMERIC ,
        "source" TEXT ,
        "State" TEXT ,
        "PlotKey" TEXT
        )
        """

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.gap_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)

        """ fieldtype check """
        for i in self.checked_df.columns:
            if self.checked_df[i].dtype!=self.datalpi_dtypes[i]:
                self.checked_df[i] = self.typecast(df=self.checked_df,field=i,fieldtype=self.datalpi_dtypes[i])

    def send_to_pg(self):

        """ has table fields """
        self.initial_dataframe.to_sql('dataHeader', self.engine, index=False, dtype=self.sqlalchemy_types)

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
