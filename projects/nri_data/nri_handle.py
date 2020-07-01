
import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
# from utils import db, sql_str, config, Acc
from utils.tools import db
from sqlalchemy import create_engine, DDL
import sqlalchemy_access as sa_a
from psycopg2 import sql
from tqdm import tqdm
from datetime import date
import urllib
import pyodbc as pyo

from projects.nri_data.nri_tools.helpers import header_fetch, extract_fields

class df_build:

    _dbkey = {
        'RangeChange2004-2008':3,
        'RangeChange2009-2015':2,
        'range2011-2016':1,
        'rangepasture2017_2018':4
    }
    dbkey = None

    def __init__(self, path, dbkey):
        """
        usage:
        -> instance = df_builder_for_2004('target_directory', 1)
            - populates tablelist

        'Raw data dump' does not exist here, so self.realp is unnecessary

        """
        """
        vacuuming
        """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.fields_dict = {}
        self.dfs = {}
        self.dbkcount = 0


        """

        """
        self.path = path # setting path
        self.mainp = os.path.dirname(os.path.dirname(path))
        # self.realp = os.path.join(path,'Raw data dump')
        self.expl = os.listdir(self.mainp)[-1]
        self.df = pd.read_csv(os.path.join(self.mainp, self.expl))
        self.dbkeys = {key:value for (key,value) in enumerate([i for i in self.df['DBKey'].unique()])}
        if dbkey in self._dbkey.keys():
            self.dbkey = self._dbkey[dbkey]

        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{self.dbkeys[dbkey]}']['TABLE.NAME'].unique()]

        self.fields_dict = extract_fields(self.path, self.dbkeys[dbkey], self.tablelist)

    def clear(self,var):
        var = None
        return var

    # def extract_fields(self, findable_string):
    #     """
    #     usage:
    #        instance = df_builder_for_2004('target_directory', 1)
    #     -> instance.extract_fields('2004')
    #         - populates instance.fields_dict with fields
    #        instance.append_fields('RangeChange2004')
    #
    #
    #     """
    #     for file in os.listdir(self.path):
    #         if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
    #             header = header_fetch(self.path)
    #             header.pull(file)
    #             self.fields_dict.update({'pointcoordinates':header.fields})
    #
    #         if (file.find('2004')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
    #             for table in self.tablelist:
    #                 header = header_fetch(self.path)
    #                 header.pull(file, table)
    #                 self.fields_dict.update({f'{table}':header.fields})
    #
    #
    # def append_fields(self, findable_string):
    #     """
    #     usage:
    #        instance = df_builder_for_2004('target_directory', 1)
    #        instance.extract_fields('2004')
    #     -> instance.append_fields('RangeChange2004')
    #         - populates instance.dfs with all the dataframes
    #     """
    #
    #     for file in os.listdir(self.path):
    #         if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False):
    #             for item in os.listdir(os.path.join(self.path,file)):
    #                 if item.find('pointcoordinates')!=-1:
    #                     tempdf =pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False, names=self.fields_dict['pointcoordinates'])
    #
    #                     t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
    #                     fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
    #                     for field in tempdf.columns:
    #                         if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
    #                             tempdf[field] = tempdf[field].apply(lambda i: i.strip())
    #                             tempdf[field] = pd.to_numeric(tempdf[field])
    #
    #                         if field in fix_longitudes:
    #                             tempdf[field] = tempdf[field].map(lambda i: i*(-1))
    #
    #
    #                     if 'COUNTY' in tempdf.columns:
    #                         tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
    #
    #                     if 'STATE' in tempdf.columns:
    #                         tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
    #
    #                     less_fields = ['statenm','countynm']
    #                     if os.path.splitext(item)[0] not in less_fields:
    #
    #                         dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
    #                         dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
    #                     tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
    #                     self.temp_coords = tempdf.copy(deep=True)
    #                     self.dfs.update({'pointcoordinates':tempdf})
    #
    #         if (file.find(findable_string)!=-1) and (file.endswith('.xlsx')==False):
    #             for item in os.listdir(os.path.join(self.path, file)):
    #                 if os.path.splitext(item)[0].upper() in self.tablelist:
    #                     tempdf = pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False,low_memory=False, names=self.fields_dict[os.path.splitext(item)[0].upper()])
    #
    #                     # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
    #                     # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
    #                     # empty spaces should automatically change into np.nan / compatible null values
    #
    #                     for field in tempdf.columns:
    #                         stay_in_varchar = ['STATE', 'COUNTY']
    #
    #                         t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
    #                         if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
    #                             tempdf[field] = tempdf[field].apply(lambda i: i.strip())
    #
    #                         if t.list[field]=="numeric" and field not in stay_in_varchar:
    #                             tempdf[field] = pd.to_numeric(tempdf[field])
    #
    #                         # for fields with dots in them..
    #                         dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6', 'NONSOIL']
    #                         if field in dot_list:
    #                             tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)
    #
    #                         ##### STRIP ANYWAY
    #                         if tempdf[field].dtype==np.object:
    #                             tempdf[field] = tempdf[field].apply(lambda i: i.strip() if type(i)!=float else i)
    #
    #
    #                     # for all tables not in "less_fields" list, create two new fields
    #
    #                         # if table has field 'COUNTY', fill with leading zeroes
    #                         if 'COUNTY' in tempdf.columns:
    #                             tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
    #                         # if table has field 'STATE', fill with leading zeroes
    #                         if 'STATE' in tempdf.columns:
    #                             tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
    #                         # create simple dbkey field
    #                         tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
    #
    #                     less_fields = ['statenm','countynm']
    #                     if os.path.splitext(item)[0] not in less_fields:
    #                         # print(item)
    #                         dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
    #                         dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
    #
    #                     if 'point' in item:
    #                         # adding landuse from points table to coords
    #                         point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
    #                         coords_dup = pd.concat([self.temp_coords,point_slice], axis=1, join="inner")
    #                         coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]
    #                         self.dfs.update({'pointcoordinates': coords_full})
    #                     self.dfs.update({f'{os.path.splitext(item)[0]}':tempdf})
d = db('nri')
d.str
