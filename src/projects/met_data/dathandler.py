import pandas as pd
import numpy as np
import sys, pandas as pd
import os, os.path
import numpy as np
from sqlalchemy import *

# from tqdm import tqdm
class datReader:
    arrays = []
    path = None
    header = None
    df = None
    engine = create_engine(os.environ['DBSTR'])
    correct_cols =['TIMESTAMP', 'RECORD', 'Switch', 'AvgTemp_10M_DegC', 'AvgTemp_4M_DegC',
       'AvgTemp_2M_DegC', 'AvgRH_4m_perc', 'Total_Rain_mm', 'WindDir_deg',
       'MaxWS6_10M_m/s', 'MaxWS5_5M_m/s', 'MaxWS4_2.5M_m/s', 'MaxWS3_1.5M_m/s',
       'MaxWS2_1M_m/s', 'MaxWS1_0.5M_m/s', 'StdDevWS2_1M_m/s',
       'AvgWS6_10M_m/s', 'AvgWS5_5M_m/s', 'AvgWS4_2.5M_m/s', 'AvgWS3_1.5M_m/s',
       'AvgWS2_1M_m/s', 'AvgWS1_0.5M_m/s', 'Sensit_Tot', 'SenSec']

    def __init__(self, path):
        """
        prepping the multirow header
        1. reads the first 4 lines of the .dat file
        2. gets each line into an list of lists
        3. appends 16 spaces to the first list
        4. create tuples with the list of lists
        5. create a multiindex with those tuples
        6. append created header to headless df

        """
        self.path = None
        self.arrays = []
        self.header = None
        self.df = None
        # read the first 4 lines
        with open(path, 'r') as reader:
            all_lines = reader.readlines()
            for each_line in all_lines[:4]:
                split_line = each_line.split(",")
                self.arrays.append([each_character.replace("\"","") for each_character in split_line])
        self.path = path
        while len(self.arrays[0])<25:
            temp_space = ''
            self.arrays[0].append(temp_space)

        self.arrays = self.arrays[1]
        for n,i in enumerate(self.arrays):
            if '%' in self.arrays[n]:
                self.arrays[n]=i.replace('%', 'perc')


        self.df = pd.read_table(path, sep=",", skiprows=4, low_memory=False)

        self.df.columns = self.arrays

    def getdf(self):

        # print("fixing '\\n' in field names..")
        for i in self.df.columns:
            if '\n' in i:
                self.df.rename(columns={f'{i}':'{0}'.format(i.replace("\n",""))}, inplace=True)
        # print("fixing float precision..")
        for i in self.df.columns:
            if self.df[i].dtype =='float64':
                self.df.round({f'{i}':6})
        # print("fixing slash characters in field name..")
        for i in self.df.columns:
            # print(i,"pre slash taker")
            if '/' in i:
                self.df.rename(columns={f'{i}':'{0}'.format(i.replace("/","_"))}, inplace=True)
            # print(i, "post slash")
        # print("fixing '.' characters in field name..")
        for i in self.df.columns:
            if '.' in i:
                self.df.rename(columns={f'{i}':'{0}'.format(i.replace(".",""))}, inplace=True)
        # print("casting timestamp as datetime..")
        for i in self.df.columns:
            if 'TIMESTAMP' in i:
                self.df.TIMESTAMP = pd.to_datetime(self.df.TIMESTAMP)

        for i in self.df.columns:
            if 'Albedo_Avg' in i:
                self.df.Albedo_Avg = self.df.Albedo_Avg.astype(float)
                self.df.Albedo_Avg = self.df.Albedo_Avg.apply(lambda x: np.nan if (x==np.inf) or (x==-np.inf) else x)


        # with self.engine.connect() as con:
        #     cols = []
        #     res = con.execute(f'SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \'{"MET_data"}\';')
        #     for i in res:
        #         print(i)
        #         cols.append(i[0])
        if 'JER' in self.path:
            # print('jer in path')
            # for i in self.df.columns:
            #
            #     if (i not in self.correct_cols) and (i not in cols):
            #         self.correct_cols.append(i)
            #         self._adding_column(i)
            #     else:
            #         print("skipped adding columns as they were already in postgres")
            #         pass
            return self.df
        else:
            # print(f'not jer, {self.path} instead')
            for i in range(0,len(self.df.columns)):
                if (self.df.columns[i]!=self.correct_cols[i])==True:
                    self.df.rename(columns={f"{self.df.columns[i]}":f"{self.correct_cols[i]}"}, inplace=True)
            return self.df


def met_batcher(path):
    df_dict = {}
    folderlist = os.listdir(path)
    count=1
    d = db("met")
    for i in folderlist:
        local_path = pathfinder(p,i)
        proj_key = i
        ins = datReader(local_path)
        tempdf = ins.getdf()
        tempdf['ProjectKey'] = proj_key
        # dat_updater(tempdf)
        df_dict.update({count:tempdf}) if '2' not in proj_key else None
        count+=1
    prefix = pd.concat([i[1] for i in df_dict.items()])
    finaldf = type_fix(prefix)
    if tablecheck("met_data", "met"):
        ingesterv2.main_ingest(finaldf,"met_data",d.str, 100000)
    else:
        table_create(finaldf, "met_data", "met")
        ingesterv2.main_ingest(finaldf,"met_data",d.str, 100000)

def type_fix(df):
    for i in df.columns:
        if (df[i].dtype == "object") and "ProjectKey" not in i:
            df[i] = df[i].astype(float)
    return df


def pathfinder(basepath,tablename):
    for folder in os.listdir(basepath):
        dir_low = os.path.join(basepath, folder)
        for item in os.listdir(dir_low):
            if tablename in item:
                final_path = os.path.normpath(os.path.join(dir_low,item))
                return final_path
