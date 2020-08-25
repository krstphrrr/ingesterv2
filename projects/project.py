import pandas as pd
import numpy as np
from projects.tables.project_tables import fields_dict
from projects.dima.tabletools import table_create, sql_command, tablecheck


# need to use an unified type_translate across code
type_translate = {
    np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)'
}


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


def update_project():
    tempdf = template()
    # check if table exists
        # if yes,update pg
    if tablecheck("project", "dima"):
        update = read_template(path,tempdf)
        #check if project key exists

    # if no, create table and update pg
    else:
        pass
def project_key_check():
    pass
