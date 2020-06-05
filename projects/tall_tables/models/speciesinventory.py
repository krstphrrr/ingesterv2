import os, os.path, pandas as pd
from sqlalchemy import *

class dataspeciesinventory:
    engine = None
    initial_dataframe = None
    # geo_dataframe = None
    dataspeciesinventory_types = {
        "LineKey" : VARCHAR(100),
        "RecKey" : VARCHAR(100),
        "DateModified" : DATE(),
        "FormType" : TEXT(),
        "FormDate" : DATE(),
        "Observer" : TEXT(),
        "Recorder" : TEXT(),
        "DataEntry" : TEXT(),
        "DataErrorChecking" : TEXT(),
        "SpecRichMethod" : NUMERIC(),
        "SpecRichMeasure" : NUMERIC(),
        "SpecRichNbrSubPlots" : NUMERIC(),
        "SpecRich1Container" : NUMERIC(),
        "SpecRich1Shape" : NUMERIC(),
        "SpecRich1Dim1" : NUMERIC(),
        "SpecRich1Dim2" : NUMERIC(),
        "SpecRich1Area" : NUMERIC(),
        "Notes" : TEXT(),
        "DateLoadedInDb" : DATE(),
        "PrimaryKey" : VARCHAR(100),
        "DBKey" : TEXT(),
        "Species" : TEXT(),
        "source" : TEXT(),
        "SpeciesCount" : VARCHAR(100),
        "Density" : NUMERIC(),
        "Plotkey" : TEXT()
        }

    def __init__(self,path):

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        self.initial_dataframe = None
        # self.geo_dataframe = None

        """ prepping a geodf from path """
        self.initial_dataframe = pd.read_csv(path)


    def send_to_pg(self):

        self.initial_dataframe.to_sql('geoSpecies', self.engine, index=False, dtype=self.dataspeciesinventory_types)
