import os, os.path
# os.chdir(os.path.join(os.getcwd(),'src')) if os.path.basename(os.getcwd())!='src' else None
import pandas as pd
import numpy as np
from sqlalchemy import *
from src.utils.tools import db
# from src.projects.tables.project_tables import fields_dict
from src.projects.dima.tabletools import table_create, sql_command, tablecheck
from src.utils.arcnah import arcno
from src.projects.aero.aero_table import fields_dict

# engine_conn_string("aero")
# df = template()
# p = r"C:\Users\kbonefont\Desktop\aero_flux_2"
# read_template(p, df)

def engine_conn_string(string):
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'


def send_model(df):
    eng = create_engine(engine_conn_string("aero"))
    df.to_sql(con=eng, name="ModelRuns", if_exists="append", index=False)

def template():
    """ creating an empty dataframe with a specific
    set of fields and field types
    """
    df = pd.DataFrame(fields_dict)
    return df

def read_template(dir, maindf):
    """ creates a new dataframe with the data on the metadata excel file,
    appending it to an empty dataframe be uploaded to the projects table in pg.
    """
    maindfcopy = maindf.copy()
    maindf.drop(maindf.index,inplace=True)
    for path in os.listdir(dir):
        if os.path.splitext(path)[1]==".xlsx":
            df = pd.read_excel(os.path.join(dir,path))
            data = df.iloc[0,:]

        elif os.path.splitext(path)[1]!=".xlsx":
            pass
        else:
            print("No metadata '.xlsx'(excel) file found within directory. Please provide project metadata file.")
    maindf.loc[len(maindf),:] = data
    return maindf

# update_model(p, 'A20200831')
def update_model(path_in_batch,modelrunkey):
    """ ingests a project metadata file if project key does not exist.

    - would be better if it would automatically pull projectkey from
    the excel file is handling to check
    - projectkey is made up of what?
    - creates datafrmae from excel metadata table
    - adds projectkey field
    - ingests/updates project table in pg

    1. check if table exists, if not create it
    2. check if project key exists, if not update it

    """
    tempdf = template()

    # check if table exists
    if tablecheck("ModelRuns", "aero"):
        if modelrun_key_check(modelrunkey):
            print(f"modelrunkey exists, aborting 'ModelRuns' update with ModelRunKey = {modelrunkey}.")
        else:
            update = read_template(path_in_batch,tempdf)
            # update['ModelRunKey'] = modelrunkey
            send_model(update)

    # if no, create table and update pg
    else:
        table_create(tempdf,"ModelRuns","aero")
        add_modelrunkey_to_pg()
        update = read_template(path_in_batch, tempdf)
        # tempdf = read_template(path_in_batch,tempdf)
        # update['ModelRunKey'] = modelrunkey
        send_model(update)


def modelrun_key_check(modelrunkey):
    d = db("aero")
    if tablecheck("ModelRuns", "aero"):
        try:
            con = d.str
            cur = con.cursor()
            exists_query = '''
            select exists (
                select 1
                from "ModelRuns"
                where "ModelRunKey" = %s
            )'''
            cur.execute (exists_query, (modelrunkey,))
            return cur.fetchone()[0]

        except Exception as e:
            print(e, "error selecting modelruns table.")
            con = d.str
            cur = con.cursor()
    else:
        print("ModelRun table does not exist.")


def add_modelrunkey_to_pg():
    d = db("aero")
    add_query = '''
        ALTER TABLE IF EXISTS "ModelRuns"
        ADD COLUMN "ModelRunKey" TEXT;
        '''
    try:
        con = d.str
        cur = con.cursor()
        cur.execute(add_query)
        con.commit()

    except Exception as e:
        print(e, 'error adding column to modelruns table')
        con = d.str
        cur = con.cursor()





#
