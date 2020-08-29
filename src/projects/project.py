import pandas as pd
import numpy as np
from sqlalchemy import *
from utils.tools import db
from projects.tables.project_tables import fields_dict
from projects.dima.tabletools import table_create, sql_command, tablecheck


# need to use an unified type_translate across code
type_translate = {
    np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)',
    np.dtype('')
}


"""
pulling db.str
configparser to create connection string for sqlalchemy engine
"""

def send_proj(df):
    eng = create_engine(engine_conn_string("dima"))
    df.to_sql(con=eng, name="project", if_exists="append", index=False)

def template():
    """ creating an empty dataframe with a specific
    set of fields and field types
    """
    df = pd.DataFrame(fields_dict)
    return df

def read_template(path, maindf):
    """ creates a new dataframe with the Value column values,
    appending it to a fed in
    """
    df = pd.read_excel(path)
    data = [i for i in df.Value]
    maindf.loc[len(maindf),:] = data


def update_project(projectkey):
    """ ingests a project metadata file if project key does not exist.

    - would be better if it would automatically pull projectkey from
    the excel file is handling to check
    - projectkey is made up of what?

    1. check if table exists, if not create it
    2. check if project key exists, if not update it

    """
    tempdf = template()
    # check if table exists
    if tablecheck("project", "dima"):
        if project_key_check(projectkey):
            print("projectkey exists, aborting ingest")
        else:
            update = read_template(path,tempdf)
            send_proj(update)

    # if no, create table and update pg
    else:
        table_create(tempdf,"project","dima")
        update = read_template(path,tempdf)
        update.to_sql(con=eng, name="project", if_exists="append", index=False)

        pass


def project_key_check(projectkey):
    d = db("dima")
    try:
        con = d.str
        cur = con.cursor()
        exists_query = '''
        select exists (
            select 1
            from project
            where "curator_PersonName" = %s
        )'''
        cur.execute (exists_query, (projectkey,))
        return cur.fetchone()[0]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()
