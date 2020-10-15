import os, os.path
# os.chdir(os.path.join(os.getcwd(),'src')) if os.path.basename(os.getcwd())!='src' else None
import pandas as pd
import numpy as np
from sqlalchemy import *
from src.utils.tools import db
from src.projects.tables.project_tables import fields_dict
from src.projects.dima.tabletools import table_create, sql_command, tablecheck
from src.utils.arcnah import arcno

type_translate = {np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)',}


"""
pulling db.str
configparser to create connection string for sqlalchemy engine
"""
def engine_conn_string(string):
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'

def send_proj(df):
    eng = create_engine(engine_conn_string("dima"))
    df.to_sql(con=eng, name="Projects", if_exists="append", index=False)

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

            data = [i for i in df.Value]

        elif os.path.splitext(path)[1]!=".xlsx":
            pass
        else:
            print("No metadata '.xlsx'(excel) file found within directory. Please provide project metadata file.")
    maindf.loc[len(maindf),:] = data
    return maindf


def update_project(path_in_batch,projectkey):
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
    if tablecheck("Projects", "dima"):
        if project_key_check(projectkey):
            print("projectkey exists, aborting ingest")
        else:
            update = read_template(path_in_batch,tempdf)
            update['projectKey'] = projectkey
            send_proj(update)

    # if no, create table and update pg
    else:
        table_create(tempdf,"Projects","dima")
        add_projectkey_to_pg()
        update = read_template(path_in_batch, tempdf)
        # tempdf = read_template(path_in_batch,tempdf)
        update['projectKey'] = projectkey
        send_proj(update)


def project_key_check(projectkey):
    d = db("dima")

    try:
        con = d.str
        cur = con.cursor()
        exists_query = '''
        select exists (
            select 1
            from Projects
            where "projectKey" = %s
        )'''
        cur.execute (exists_query, (projectkey,))
        return cur.fetchone()[0]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


def add_projectkey_to_pg():
    d = db("dima")
    add_query = '''
        ALTER TABLE IF EXISTS "Projects"
        ADD COLUMN "projectKey" TEXT;
        '''
    try:
        con = d.str
        cur = con.cursor()
        cur.execute(add_query)
        con.commit()

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()
