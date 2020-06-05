# modular = 1
import os, os.path, pandas as pd
from sqlalchemy import *
from utils.tools import db
from projects.tall_tables.talltables_handler import field_parse, sql_command

class datalpi:
    engine = None
    initial_dataframe = None
    checked_df = None
    typedict = {
        "LineKey" : "v_100",
        "RecKey" : "v_100",
        "DateModified" : "date",
        "FormType" : "text",
        "FormDate" : "date",
        "Observer" : "text",
        "Recorder" : "text",
        "DataEntry" : "text",
        "DataErrorChecking" : "text",
        "Direction" : "v_50",
        "Measure" : "float",
        "LineLengthAmount" : "float",
        "SpacingIntervalAmount" : "float",
        "SpacingType" : "text",
        "HeightOption" : "text",
        "HeightUOM" : "text",
        "ShowCheckbox" : "float",
        "CheckboxLabel" : "text",
        "PrimaryKey" : "v_100",
        "DBKey" : "text",
        "PointLoc" : "float",
        "PointNbr" : "float",
        "ShrubShape" : "text",
        "layer" : "text",
        "code" : "text",
        "chckbox" : "int",
        "source" : "text",
        "STATE" : "v_50",
        "SAGEBRUSH_SPP": "text",
        "PLOTKEY":"v_100"
        }
    sqlalchemy_types = None
    pandas_dtypes = None
    psycopg2_types = None
    psycopg2_command = None

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        self.sqlalchemy_types = None
        self.pandas_dtypes = None
        self.psycopg2_types = None
        self.psycopg2_command = None
        self.checked_df =None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path, low_memory=False)
        self.checked_df = self.initial_dataframe.copy()

        """ creating type dictionaries """
        self.sqlalchemy_types = field_parse('sqlalchemy', self.typedict)
        self.pandas_dtypes = field_parse('pandas', self.typedict)
        self.psycopg2_types = field_parse('pg', self.typedict)
        self.psycopg2_command = sql_command(self.psycopg2_types,name)


    def checked(self):
        """ fieldtype check """
        for i in self.checked_df.columns:
            if self.checked_df[i].dtype!=self.pandas_dtypes[i]:
                self.checked_df[i] = self.typecast(df=self.checked_df,field=i,fieldtype=self.pandas_dtypes[i])

    def typecast(self,df,field,fieldtype):
        data = df
        castfield = data[field].astype(fieldtype)
        return castfield

    def send_to_pg(self):

        self.initial_dataframe.to_sql('dataLPI', self.engine, index=False, dtype=self.sqlalchemy_types)

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
