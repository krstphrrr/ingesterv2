import pandas as pd
from src.utils.tools import db
from src.utils.arcnah import arcno
import os
from psycopg2 import sql
import numpy as np
from src.projects.dima.tablefields import tablefields
# from src.projects.aero.aero_model import engine_conn_string
# from src.projects.dima.tabletools import tablecheck, csv_fieldcheck

def engine_conn_string(string):
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'

def fix_fields(df : pd.DataFrame, keyword: str, debug=None):
    """ Checks for duplicate fields produced by primarykey joins


    """
    df = df.copy()
    done=False
    while done!=True:
        if len([i for i in df.columns if keyword in i])>=3:
            print(f'0.1, {keyword} field occurs 3 times, dropping both additional iterations') if debug else None
            df.drop([f'{keyword}_y',f'{keyword}_x'], axis=1, inplace=True)
            done=True
            return df
        else:
            if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
                if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                    # if the two notes are the same, keep one of them.
                    print(f'1. {keyword}_x equals y (drops y)') if debug else None
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df


                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())>len(df[f'{keyword}_y'].unique())):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'2. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is larger than y.unique (deletes y)') if debug else None
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())<len(df[f'{keyword}_y'].unique())):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'3. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is smaller than y.unique (deletes x)') if debug else None
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the x is none, keep the y
                    print(f'4. {keyword}_x does not equal y, and the length of Nones in x is larger than the length of Nones in y') if debug else None
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    # df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the y is none, keep the x
                    print(f'5. {keyword}_x does not equal y, and the length of Nones in x is smaller than the length of Nones in y') if debug else None
                    df.drop(['Notes_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                    done=True
                    return df

                else:
                    print("6. both dates are unequal, but occur only in one row: dropping y")
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    return df

            else:
                return df

def new_tablename(df:pd.DataFrame):
    """
    if dataframe has an 'ItemType' field,
    return a new tablename string depending on what's present in that field.
    """
    if 'ItemType' in df.columns:
        if (any(df.ItemType=='M')) or (any(df.ItemType=='m')):
            newtablename = 'tblHorizontalFlux'
            return newtablename

        elif (any(df.ItemType=='T')) or (any(df.ItemType=='t')):
            newtablename = 'tblDustDeposition'
            return newtablename



def table_create(df: pd.DataFrame, tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """

    table_fields = {}

    try:

        for i in df.columns:
            if tablename!='aero_runs':
                if ("dima" in conn) or ("dimadev" in conn):

                    print("dima or dimadev")
                    table_fields.update({f'{i}':f'{tablefields[possible_tables[tablename]][i]}'})
                else:
                    print("other schemas")
                    table_fields.update({f'{i}':f'{type_translate[df.dtypes[i].name]}'})
                # table_fields.update({f'{i}':f'{tablefields[possible_tables[tablename]][i]}'})
            else:
                print("aero")
                table_fields.update({f'{i}':f'{aero_translate[df.dtypes[i].name]}'})


        if table_fields:
            print("checking fields")
            comm = sql_command(table_fields, tablename, conn) if conn!='nri' else sql_command(table_fields, tablename, 'nritest')
            d = db(f'{conn}')
            con = d.str
            cur = con.cursor()
            # return comm
            cur.execute(comm)
            con.commit()

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()

def alt_gapheader_check(dataframe):
    for i in dataframe.columns:
        if "PerennialsCanopy" in i:
            return dataframe
        elif "Perennials" in i:
            df = dataframe.copy()
            df.rename(columns={
                "Perennials":"PerennialsCanopy",
                "AnnualGrasses":"AnnualGrassesCanopy",
                "AnnualForbs":"AnnualForbsCanopy",
                "Other":"OtherCanopy"},
                inplace=True)
            df["PerennialsBasal"] = pd.NA
            df["AnnualGrassesBasal"] = pd.NA
            df["AnnualForbsBasal"] = pd.NA
            df["OtherBasal"] = pd.NA

            return df


def table_fields(df, tablename):
    table_fields = {}
    for i in df.columns:
        # table_fields.update({f'{i}':f'{type_translate[df.dtypes[i].name]}'})
        table_fields.update({f'{i}':f'{tablefields[possible_tables[tablename]][i]}'})
    return table_fields


possible_tables = {
    "tblBSNE_TrapCollection":"tblDustDeposition",
    "tblBSNE_Box":"tblHorizontalFlux",
    "tblHorizontalFlux":"tblHorizontalFlux",
    "tblDustDeposition":"tblDustDeposition",
    "tblGapDetail":"tblGapDetail",
    "tblGapHeader":"tblGapHeader",
    "tblLPIDetail":"tblLPIDetail",
    "tblLPIHeader":"tblLPIHeader",
    "tblLines":"tblLines",
    "tblPlots":"tblPlots",
    "tblSpecies":"tblSpecies",
    "tblSpeciesGeneric":"tblSpeciesGeneric",
    "tblSites":"tblSites",
    "Projects":"Projects",
    "tblSoilPits":"tblSoilPits",
    "tblSoilPitHorizons":"tblSoilPitHorizons",
    "tblSoilStabDetail":"tblSoilStabDetail",
    "tblSoilStabHeader":"tblSoilStabHeader",
    "tblPlantDenDetail":"tblPlantDenDetail",
    "tblPlantDenHeader":"tblPlantDenHeader",
    "tblPlantProdDetail":"tblPlantProdDetail",
    "tblPlantProdHeader":"tblPlantProdHeader",
    "tblSpecRichHeader":"tblSpecRichHeader",
    "tblSpecRichDetail":"tblSpecRichDetail",
    "tblPlotNotes":"tblPlotNotes",
    "tblQualDetail":"tblQualDetail",
    "tblQualHeader":"tblQualHeader",
    "schemaTable":"schemaTable"
}


def sql_command(typedict:{}, name:str, db:str=None):
    """
    create a string for a psycopg2 cursor execute command to create a new table.
    it receives a dictionary with fields and fieldtypes, and builds the string
    using them.

    """
    db_choice={
    # dimas in postgres
    "dima":"postgres",
    # met data in gisdb
    "met":"met_data",
    # tall tables
    "gisdb": "gisdb",
    "geo":"gisdb",
    # nri
    "nri": "nritest",
    "dimadev":"postgres",
    "aero":"aero_data",
    }
    schema_choice={
    "dima":"public",
    "met":"public",
    "gisdb":"public",
    "nri":"public",
    "dimadev":"dimadev",
    "aero":"public",
    "geo":"public"
    }
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" \
     (""" if db==None else f""" CREATE TABLE {db_choice[db]}.{schema_choice[db]}.\"{name}\" ("""
    try:
        for i,x in enumerate(inner_list):
            if i==len(inner_list)-1:
                part_1+=f"{x}"
            else:
                part_1+=f"{x},"
    except Exception as e:
        print(e)
    finally:
        part_1+=");"
        return part_1

def tablecheck(tablename, conn="dima"):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    tableschema = "dimadev" if conn=="dimadev" else "public"
    try:
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()

def csv_fieldcheck(df: pd.DataFrame, path: str, table: str):
    checked = 0
    try:
        escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
        for col in df.columns:
            if df.dtypes[col] == 'object':
                for v, e in escaped.items():
                    df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
                    checked = 1
    except Exception as e:
        print(e)

    finally:
        if checked==1:
            df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))
        else:
            print('fields not fixed; csv export aborted')

def drop_dbkey(table, path):
    squished_path = os.path.split(os.path.splitext(path)[0])[1].replace(" ","")
    d = db('dima')
    try:
        # print(f'"DELETE FROM postgres.public.{table} WHERE \"DBKey\"=\'{squished_path}\';"')
        con = d.str
        cur = con.cursor()
        cur.execute(
            sql.SQL("DELETE FROM postgres.public.{0} WHERE \"DBKey\"= '%s';" % squished_path).format(
                sql.Identifier(table))
        )
        con.commit()
        print(f'successfully dropped \'{squished_path}\' from table \'{table}\'')
    except Exception as e:
        con = d.str
        cur = con.cursor()
        print(e)


def blank_fixer(df):
    for i in df.columns:
        if df[i].dtype=='object':
            df[i].replace('',np.nan,inplace=True)
    return df

def significant_digits_fix_pandas(df):
    colist = ['sedimentGperDay','emptyWeight', 'recordedWeight', 'sedimentWeight']
    for i in df.columns:
        if i in colist:
            df[i] = df[i].apply(lambda x: round(x,4))
        else:
            return df
    return df

def northing_round(dataframe):
    df = dataframe.copy()
    for i in df.columns:
        if "Northing" in i:
            df[i] = df[i].apply(lambda x: round(x,6))
    return df



def float_field(df, field):
    temp_series = df[field].astype('float64')
    return temp_series

def openingsize_fixer(df):
    for i in df.columns:
        if 'openingSize' in i:
            df[i] = df[i].astype('float64')
    return df

def datetime_type_assert(df):
    for i in df.columns:
        if df.dtypes[i]=="datetime64[ns]" or "date" in i.lower():
            df[i] = df[i].apply(lambda x: pd.NaT if x is None else x).astype("datetime64[ns]")
    return df

def dateloadedcheck(df):
    df = df.copy()
    for i in df.columns:
        if "DateLoadedInDB" in i:
            df.DateLoadedInDB = df.DateLoadedInDB.astype("datetime64[ns]")
        else:
            pass
    return df

type_translate = {
    'int64':'int',
    'Int64':'int',
    "object":'text',
    'datetime64[ns]':'timestamp',
    'bool':'boolean',
    'float64':'float(5)'
}

aero_translate = {
    'int64':'int',
    'Int64':'int',
    "object":'text',
    'datetime64[ns]':'timestamp',
    'bool':'boolean',
    'float64':'float'
}
"""
TOOL TO SURGICALLY DELETE ROWS FROM DIMA DATABASE
"""
def keyvalAdder(dict):
    start =''
    end = 'WHERE '
    for key,value in dict.items():
        if key!=list(dict.keys())[-1]:
            start+=f'"{key}" = \'{value}\' AND '
        else:
            start+=f'"{key}" = \'{value}\''
    end+=start
    return end


def rowDeleter(tablename:str,dev=False,**fields:str):

    # where to delete from: dev or public
    print(f"amount of key-value pairs:{len(fields)}")
    if dev==False:
        d = db('dima')
        keyword = "public"
    elif dev==True:
        d = db("dimadev")
        keyword = "dimadev"
    # creating connection/cursor object (for dev or public)
    con = d.str
    cur = con.cursor()
    # how to structure SQL depending on number of key-value pairs
    if len(fields)<2:
        print(f"deleting 1 key-value pair from postgres.{keyword}.{tablename}")
        for key,value in fields.items():
            singleField = f'''where "{key}"=\'{value}\';'''
            printOut = f' "{key}" = \'{value}\''
        # return f'DELETE FROM postgres.public."{tablename}" {singleField}'
        try:
            sqlStart = f"DELETE FROM postgres.{keyword}.\"{tablename}\" {singleField}"
            print(f"Removing {printOut}...")
            print(f"Using SQL verb : {sqlStart}")
            cur.execute(sqlStart)
            print("Done.")
            con.commit()

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()

    elif len(fields)>=2:
        print(f"deleting {len(fields)} key-value pairs from postgres.{keyword}.{tablename}")
        endVerb = keyvalAdder(fields)
        # return f'DELETE FROM postgres.public."{tablename}" {endVerb};'
        try:
            sqlStart = f"DELETE FROM postgres.{keyword}.\"{tablename}\" {endVerb};"
            print(f"Removing rows using various key-value pairs...")
            print(f"Using SQL verb : {sqlStart}")
            cur.execute(sqlStart)
            print("Done.")
            con.commit()

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()

"""
TOOL TO EXPORT TABLES FROM PG TO LOCAL CSV
(POST-INGESTION QC)

"""
def pg2csvExport(tablename=None,dev=False,dbkey=None):
    path = os.getcwd()
    # where to delete from: dev or public
    if dev==False:
        d = db('dima')
        keyword = "public"
        conn = "dima"
    elif dev==True:
        d = db("dimadev")
        keyword = "dimadev"
        conn = "dimadev"
    # creating connection/cursor object (for dev or public)
    engine = engine_conn_string(conn)
    # con = d.str
    # cur = con.cursor()
    nopk = "DBKey"
    if "tblPlots" in tablename:
        nopk = "ProjectKey"
    else:
        nopk = "DBKey"

    if tablecheck(tablename,conn=conn):
        if (dbkey is not None) and (tablename is not None):
            # both table and projectkey
            print(f"fetching pgdata that includes:{tablename}, {dbkey}...")
            sql = f"SELECT * from postgres.{keyword}.\"{tablename}\" where \"{nopk}\" = '{dbkey}'"
            df = pd.read_sql(sql, con=engine)
            csv_fieldcheck(df,path,tablename)
            print("done.")
        elif (dbkey is None) and (tablename is not None):
            # only table
            print(f"fetching pgdata that includes:{dbkey}...")
            sql = f"SELECT * from postgres.{keyword}.\"{tablename}\""
            df = pd.read_sql(sql,con=engine)
            csv_fieldcheck(df,path,tablename)
            print("done.")

        elif (dbkey is not None) and (tablename is None):
            print("csv all tables with x projectkey;not implemented")
        else:
            print("no table, no projectkey; not implemented")
    else:
        print(f"{tablename} does not exist in {conn} database")


def schemaCreate(path, tablename):
    """
    ingesting tables with less priority: schemaTable
    """
    d = db("geo")
    df = pd.read_excel(path)
    if tablecheck(tablename, conn="geo"):
        print("table exists; ingesting...")
        ingesterv2.main_ingest(df,tablename,d.str)
        print("done")

    else:
        print("table does not exists; creating table..")
        table_create(df,tablename,conn="geo")
        print("ingesting...")
        ingesterv2.main_ingest(df,tablename, d.str)
        print("done")

def colcheck(tablename, conn):
    """
    incomplete implementation: check ingested fields vs schema on pg
    """
    sql=f""" SELECT *
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name   = '{tablename}'
             ;
        """
    d = db(f'{conn}')
    if tablecheck(tablename,conn):
        con = d.str
        cur = con.cursor()
        try:
            cur.execute(sql)
            return [i[3] for i in cur.fetchall()]

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()
    else:
        print("table does not exist")

def project_field_change(
                field_2_change:str,
                value_2_change:str,
                new_value:str,
                database:str,
                whichtable:str) -> None:
    """
    replace all values of a given field with another value.

    Parameters
    ----------
    field_2_change: string
    value_2_change: string
    new_value: string
    database: string

    Returns
    -------
    None
    """
    dbchoice = {
        "dima":"public",
        "dimadev":"dimadev"
        }
    d = db(database)

    sql = f'''
        UPDATE
            {dbchoice[database]}."{whichtable}"
        SET
            "{field_2_change}" = REPLACE(
                "{field_2_change}",
                \'{value_2_change}\',
                \'{new_value}\'
            );
        '''


    try:
        con = d.str
        cur = con.cursor()
        cur.execute(sql)
        con.commit()

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()
