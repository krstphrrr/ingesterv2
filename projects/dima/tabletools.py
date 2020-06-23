import pandas as pd
from utils.tools import db
from utils.arcnah import arcno

def fix_fields(df : pd.DataFrame, keyword: str):
    df = df.copy()
    done=False
    while done!=True:
        # for i in range(0, len(df.columns)):
        #     if df.columns[i]=="Notes":
        #         print('dropped notes')
        #         df.drop(["Notes"],axis=1, inplace=True)
        #     else:
        #         pass

        if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
            if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                # if the two notes are the same, keep one of them.
                print(f'1. {keyword}_x equals y (drops y)')
                df.drop([f'{keyword}_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df


            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())>len(df[f'{keyword}_y'].unique())):
                # if the two notes are different AND the x is none, keep the y
                print(f'2. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is larger than y.unique (deletes y)')
                df.drop([f'{keyword}_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())<len(df[f'{keyword}_y'].unique())):
                # if the two notes are different AND the x is none, keep the y
                print(f'3. {keyword}_x does not equal y, and \'None\' is not in column x or y, and the length of x.unique is smaller than y.unique (deletes x)')
                df.drop([f'{keyword}_x'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                # if the two notes are different AND the x is none, keep the y
                print(f'4. {keyword}_x does not equal y, and the length of Nones in x is larger than the length of Nones in y')
                df.drop([f'{keyword}_x'], axis=1, inplace=True)
                # df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                # if the two notes are different AND the y is none, keep the x
                print(f'5. {keyword}_x does not equal y, and the length of Nones in x is smaller than the length of Nones in y')
                df.drop(['Notes_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df

        else:
            return df

def new_tablename(df:pd.DataFrame):
    """
    if dataframe has an 'ItemType' field,
    return a new tablename string depending on what's present in that field.
    """
    if 'ItemType' in df.columns:
        if (df.ItemType[0]=='M') or (df.ItemType[0]=='m'):
            newtablename = 'tblHorizontalFlux'
            return newtablename

        elif (df.ItemType[0]=='T') or (df.ItemType[0]=='t'):
            newtablename = 'tblDustDeposition'
            return newtablename

def table_create(df: pd.DataFrame, tablename: str):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    type_translate = {
        'int64':'int',
        "object":'text',
        'datetime64[ns]':'timestamp',
        'bool':'boolean',
        'float64':'float'
    }
    table_fields = {}

    try:
        for i in df.columns:
            # print(df[i].dtype)
            table_fields.update({f'{i}':f'{type_translate[df.dtypes[i].name]}'})

        if table_fields:
            comm = sql_command(table_fields, tablename)
            d = db('dima')
            con = d.str
            cur = con.cursor()
            # return comm
            cur.execute(comm)
            con.commit()

    except Exception as e:
        print(e)
        d = db('dima')
        con = d.str
        cur = con.cursor()

def sql_command(typedict:{}, name:str):
    """
    create a string for a psycopg2 cursor execute command to create a new table.
    it receives a dictionary with fields and fieldtypes, and builds the string
    using them.
    """
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE postgres.public.\"{name}\" ("""
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

def tablecheck(tablename):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    try:
        d = db('dima')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (f'{tablename}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db('dima')
        con = d.str
        cur = con.cursor()
