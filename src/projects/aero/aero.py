import os, os.path
import time, datetime
os.chdir(os.path.join(os.getcwd(),'src')) if os.path.basename(os.getcwd())!='src' else None
import pandas as pd
from utils.tools import db
from projects.dima.tabletools import table_create, sql_command, tablecheck
from projects.tall_tables.talltables_handler import ingesterv2
from projects.dima.tabletools import table_create, tablecheck

"""
X 1.read directory that holds outputs
X 2.concatenate dataframe
X 2.5 Plotid
3.choose model_run_id / ModelRunKey
- for now modelrun lookup + modeloutputs will be in dima db

4. check if lookuptable exists +
   check if model run id exists +
   update modelrun lookup table with appropriate id
4.update model_run + model run loookup table on postgres
"""
p = r"C:\Users\kbonefont\Desktop\aero_flux_output"

def txt_read(path):
    df_dict = {}
    testset = ["20184145384203B2_flux","20184145384203B1_flux","20184145374203B2_flux"]
    count = 1
    for i in os.listdir(path):
        # debug block
        # if os.path.splitext(i)[0] in [i for i in testset]:
        #     file = os.path.join(path,i)
        #     created_time = os.path.getctime(file)
        #     parsed_ctime = time.ctime(created_time)
        #     date_ctime = datetime.datetime.strptime(parsed_ctime, "%a %b %d %H:%M:%S %Y")
        #     # print(date_ctime)
        #     complete = os.path.join(path,i)
        #     temp = pd.read_table(complete, sep="\t", low_memory=False)
        #     df_dict.update({f"df{count}":temp})
        #     count+=1

        # get date/time for modelrun
        file = os.path.join(path,i)
        created_time = os.path.getctime(file)
        parsed_ctime = time.ctime(created_time)
        date_ctime = datetime.datetime.strptime(parsed_ctime, "%a %b %d %H:%M:%S %Y")
        # get plotid
        plotid = i.split('_')[0]
        complete = os.path.join(path,i)
        temp = pd.read_table(complete, sep="\t", low_memory=False)
        temp['PlotId'] = plotid
        df_dict.update({f"df{count}":temp})
        print(f"{count} added")
        count+=1
    return pd.concat([d[1] for d in df_dict.items()],ignore_index=True)



def model_run_updater(df):
    """
    1. creates a table in postgres with supplied dataframe
    2. appends data to postgres table
    """
    d = db("dima")
    if tablecheck('aero_runs'):
        print('aero_runs exists, skipping table creation')
        ingesterv2.main_ingest(df, "aero_runs", d.str,100000)
    else:
        print('creating aero_runs')
        table_create(df, "aero_runs", "dima")
        ingesterv2.main_ingest(df, "aero_runs", d.str,100000)

def model_run_create():
    pass

type_translate = {np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)',}

fields_dict = {
"ModelRunKey":pd.Series([],dtype='object'),
"Model":pd.Series([],dtype='object'),
"LocationType":pd.Series([],dtype='object'),
"SurfaceSoilSource":pd.Series([],dtype='datetime64[ns]'),
"MeteorologicalSource":pd.Series([],dtype='object'),
"ModelRunNotes":pd.Series([],dtype='object'),
}
