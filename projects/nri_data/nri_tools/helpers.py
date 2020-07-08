
import os, sqlalchemy, urllib
import pandas as pd
import numpy as np
from datetime import date
from sqlalchemy import create_engine, DDL
from win32com.client import Dispatch
from projects.nri_data.nri_tools.paths import path1_2

class header_fetch:

    def __init__(self, targetdir):
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.dir = targetdir
        self.all = [i for i in os.listdir(targetdir)]
        self.files = [i for i in os.listdir(targetdir) if i.endswith('.xlsx')==True]
        self.dirs = [i for i in os.listdir(targetdir) if os.path.splitext(i)[1]=='']

    def clear(self,var):
        var = None
        return var

    def pull(self,file, col=None):
        self.fields = []

        if (file in self.files) and (file.find('Coordinates')!=-1):
            temphead = pd.read_excel(os.path.join(self.dir,file))
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file in self.files) and ('Coordinates' not in file):
            full = pd.read_excel(os.path.join(self.dir,file))
            is_concern = full['Table name']==f'{col}'
            temphead = full[is_concern]
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file.endswith('.csv')) and ('Coordinates' not in file):
            full = pd.read_csv(os.path.join(self.dir,file))
            is_target = full['TABLE.NAME']==f'{col}'
            temphead = full[is_target]
            for i in temphead['FIELD.NAME']:
                self.fields.append(i)

        else:
            print('file is not in supplied directory')



def dbkey_gen(df,newfield, *fields):
    df[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype(str)).agg(''.join,axis=1).astype(object)

class type_lookup:

    df = None
    tbl = None
    target = None
    list = {}
    length = {}

    dbkey = {
        3:'RangeChange2004-2008',
        2:'RangeChange2009-2015',
        1:'range2011-2016',
        4:'rangepasture2017_2018'
    }

    def __init__(self,df,tablename,dbkeyindex, path):

        mainp = os.path.dirname(os.path.dirname(path))
        expl = os.listdir(os.path.dirname(os.path.dirname(path)))[-1]
        exp_file = os.path.join(mainp,expl)

        self.df = df
        self.tbl = tablename
        key = self.dbkey[dbkeyindex]

        nri_explanations = pd.read_csv(exp_file)

        is_target = nri_explanations['Table name'] == f'{tablename.upper()}'
        self.target = nri_explanations[is_target]
        for i in self.df.columns:
            temprow = self.target[(self.target['Field name']==i)]
            # temprow = self.target[(self.target['Field name']==i) & (self.target['DBKey']==key)  ]
            packed = temprow["Data type"].values
            lengths = temprow["Field size"].values
            # self.length = temprow['FIELD.SIZE']

            for j in packed:
                self.list.update({ i:f'{j}'})
            for k in lengths:
                self.length.update({i:k})


def mdb_create(output):
    try:
        dbname = f'NRI_EXPORT_{date.today().month}_{date.today().day}_{date.today().year}.mdb'
        pathname = os.path.join(output,dbname)
        accApp = Dispatch("Access.Application")
        dbEngine = accApp.DBEngine
        workspace = dbEngine.Workspaces(0)
        dbLangGeneral = ';LANGID=0x0409;CP=1252;COUNTRY=0'
        newdb = workspace.CreateDatabase(pathname, dbLangGeneral, 64)

        newdb.Execute("""CREATE TABLE Table1 (
                          ID autoincrement,
                          Col1 varchar(50),
                          Col2 double,
                          Col3 datetime);""")
    except Exception as e:
        print(e)

    finally:
        accApp.DoCmd.CloseDatabase
        accApp.Quit
        newdb = None
        workspace = None
        dbEngine = None
        accApp = None

def ret_access(whichmdb):
    MDB = whichmdb
    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
    mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(mdb_string)}"
    engine = create_engine(connection_url)
    return engine



def access_dictionary(df, tablename):
    onthefly={}
    only_once = set()
    t = type_lookup(df,tablename,1,path1_2)
    temptypes = t.list
    templengths = t.length
    def alchemy_ret(type,len=None):
        """
        function that takes a type(numeric or character+length) returns
        a sqlalchemy/pg compatible type
        """
        if (type=='numeric') and (len==None):
            return sqlalchemy.types.Float(precision=3, asdecimal=True)
        elif (type=='character') and (len!=None):
            return sqlalchemy.types.VARCHAR(length=len)
    for key in temptypes:
        """
        creating custom dictionary per table to map pandas types to pg
        """
        state_key = ["STATE", "COUNTY"]
        if key not in only_once:
            only_once.add(key)

            if temptypes[key]=='numeric':
                onthefly.update({f'{key}':alchemy_ret(temptypes[key])})
                for k in state_key:
                    if k == "STATE":
                        onthefly.update({f'{k}':alchemy_ret('character',2)})
                    if k=="COUNTY":
                        onthefly.update({f'{k}':alchemy_ret('character',3)})

            if temptypes[key]=='character':
                onthefly.update({f'{key}':alchemy_ret(temptypes[key],templengths[key])})

                if key == "PTNOTE":
                    onthefly.update({"PTNOTE":sqlalchemy.types.Text})
    return onthefly
