
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

from projects.nri_data.nri_tools.helpers import header_fetch, type_lookup, dbkey_gen
from projects.nri_data.nri_tools.table_preppers import concern, disturbance, pastureheights, soilhorizon, pintercept, practice

"""

TODO

tablebuilder:
- apply fixes per chosen dataset/table


"""


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

        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{dbkey}']['TABLE.NAME'].unique()]

        if 'rangepasture2017_2018' in dbkey:
            self.set_2018 = '.csv'
        else:
            self.set_2018 = '.xlsx'

        # exctrac_fields ==> path, which_dataset(user chooses), tablelist
        # self.fields_dict = extract_fields(self.path, self.dbkeys[dbkey], self.tablelist)

    def clear(self,var):
        var = None
        return var

    def extract_fields(self, findable_string, tname=None):
        """
        usage:
           instance = df_builder_for_2004('target_directory', 1)
        -> instance.extract_fields('2004')
            - populates instance.fields_dict with fields
           instance.append_fields('RangeChange2004')


        """
        if tname!=None:
            tname=tname.upper()
        do_not_add_raw = ['RangeChange2004-2008', 'RangeChange2009-2015']
        backuppath = self.path
        steps = 0
        while steps<2:
            if self.dbkeys[self.dbkey] not in do_not_add_raw:
                if 'Raw data dump' not in self.path:
                    self.path = os.path.join(self.path, 'Raw data dump')
                else:
                    pass
            for file in os.listdir(self.path):
                # if '2009' not in findable_string:
                if 'RangeChange2009-2015' not in self.dbkeys[self.dbkey]:
                    if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True) and (steps==0):
                        header = header_fetch(self.path)
                        header.pull(file)
                        self.fields_dict.update({'pointcoordinates':header.fields})
                        steps+=1
                else:
                    if steps<1:
                        steps+=1
                    else:
                        pass

                if (file.find(f'{findable_string}')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith(f'{self.set_2018}')==True) and (steps==1):

                    tlist = self.tablelist if tname==None else [tname]
                    for table in tlist:
                        if 'POINTCOORDINATES' not in table:
                            header = header_fetch(self.path)
                            header.pull(file, table)
                            self.fields_dict.update({f'{table}':header.fields})
                            steps+=1
            if steps>=2:
                if self.path!=backuppath:
                    self.path=backuppath


    #
    def append_fields(self, findable_string, tname=None):
        """
        usage:
           instance = df_builder_for_2004('target_directory', 1)
           instance.extract_fields('2004')
        -> instance.append_fields('RangeChange2004')
            - populates instance.dfs with all the dataframes
        """
        if tname!=None:
            tname=tname.upper()
        do_not_add_raw = ['RangeChange2004-2008', 'RangeChange2009-2015']
        backuppath = self.path
        steps = 0
        while steps<2:
            if self.dbkeys[self.dbkey] not in do_not_add_raw:
                if 'Raw data dump' not in self.path:
                    self.path = os.path.join(self.path, 'Raw data dump')
                else:
                    pass
            for file in os.listdir(self.path): # realp
                if 'RangeChange2009-2015' not in self.dbkeys[self.dbkey]:
                    if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False) and (steps==0):
                        for item in os.listdir(os.path.join(self.path,file)): #realp
                            if item.find('pointcoordinates')!=-1:
                                tempdf =pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False, names=self.fields_dict['pointcoordinates'])

                                t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, backuppath) # not realp
                                fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
                                for field in tempdf.columns:
                                    if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                        tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                                        tempdf[field] = pd.to_numeric(tempdf[field])

                                    if field in fix_longitudes:
                                        tempdf[field] = tempdf[field].map(lambda i: i*(-1))


                                if 'COUNTY' in tempdf.columns:
                                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                                if 'STATE' in tempdf.columns:
                                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')

                                less_fields = ['statenm','countynm']
                                if os.path.splitext(item)[0] not in less_fields:

                                    dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                                    dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
                                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
                                self.temp_coords = tempdf.copy()
                                self.dfs.update({'pointcoordinates':tempdf})
                                steps+=1
                else:
                    if steps<1:
                        steps+=1
                    else:
                        pass

                if (file.find(findable_string)!=-1) and (file.endswith('.xlsx')==False) and (steps==1) and ('PointCoordinates' not in file) and (file.endswith('.zip')==False):
                    for item in os.listdir(os.path.join(self.path, file)):
                        tlist = self.tablelist if tname==None else [tname]
                        if os.path.splitext(item)[0].upper() in tlist:
                            # print(self.path, item, file)

                            tempdf = pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False,low_memory=False, names=self.fields_dict[os.path.splitext(item)[0].upper()])

                            # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
                            # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
                            # empty spaces should automatically change into np.nan / compatible null values
                            # t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, backuppath)

                            for field in tempdf.columns:
                                # print(f'{item}, {field}') # debug
                                stay_in_varchar = ['STATE', 'COUNTY']
                            #
                                t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, backuppath)
                                if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                    tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                            #
                                if t.list[field]=="numeric" and field not in stay_in_varchar:
                                    if 'SAGEBRUSH_SHAPE' in field:
                                        tempdf[field] = tempdf[field].apply(lambda i: np.nan if ('.' in i) and (any([j.isdigit() for j in i])!=True) else i)
                                    tempdf[field] = pd.to_numeric(tempdf[field])

                                # for fields with dots in them..
                                dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6', 'NONSOIL']
                                if field in dot_list:
                                    tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

                                ##### STRIP ANYWAY
                                if tempdf[field].dtype==np.object:
                                    tempdf[field] = tempdf[field].apply(lambda i: i.strip() if type(i)!=float else i)


                            # for all tables not in "less_fields" list, create two new fields

                                # if table has field 'COUNTY', fill with leading zeroes
                                if 'COUNTY' in tempdf.columns:
                                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                                # if table has field 'STATE', fill with leading zeroes
                                if 'STATE' in tempdf.columns:
                                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                                # create simple dbkey field
                                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])

                            less_fields = ['statenm','countynm']
                            if os.path.splitext(item)[0] not in less_fields:
                                # print(item)
                                dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                                dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')

                            if 'point' in item and ('RangeChange2009-2015' not in self.dbkeys[self.dbkey]):
                                # adding landuse from points table to coords
                                point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
                                coords_dup = pd.concat([self.temp_coords,point_slice], axis=1, join="inner")
                                coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]
                                self.dfs.update({'pointcoordinates': coords_full})
                            self.dfs.update({f'{os.path.splitext(item)[0]}':tempdf})
                            steps+=1
                # return tempdf
            if steps>=2:
                if self.path!=backuppath:
                    self.path=backuppath

def task_parser(tablename):

    table_map = {
        'concern': ['2004','2009','2011','2013','2017'],
        'disturbance' : ['2004','2009','2011','2013','2017'],
        'ecosite':['2004'],
        'esfsg':['2009','2011','2013','2017'],
        'gintercept': ['2004','2009','2011','2013','2017'],
        'gps': ['2004','2009','2011','2013','2017'],
        'point' : ['2004','2011','2013','2017'],
        'pastureheights': ['2009','2011','2013','2017'],
        'plantcensus': ['2009','2011','2013','2017'],
        'point': ['2004','2009','2011','2013','2017'],
        'pointcoordinates': ['2004','2011','2013','2017'],
        'ptnote': ['2004','2009','2011','2013','2017'],
        'rangehealth' : ['2004','2009','2011','2017'],
        'soildisag': ['2004','2009','2011','2013','2017'],
        'soilhorizon' : ['2009','2011','2013','2017'],
        'statenm': ['2004','2009','2011','2013','2017'],
        'pintercept': ['2004','2009','2011','2013','2017'],
        'practice': ['2004','2009','2011','2013','2017'],
    }
    tablelist = []
    # task=0

    if tablename in table_map.keys():
        if '2004' in table_map[tablename]:
            a = df_build(path1_2, 'RangeChange2004-2008')
            a.extract_fields('2004',tablename)
            a.append_fields('2004',tablename)
            df1 = a.dfs[tablename]
            tablelist.append(df1)

        if '2009' in table_map[tablename]:
            b = df_build(path1_2, 'RangeChange2009-2015')
            b.extract_fields('2009', tablename)
            b.append_fields('2009',tablename)
            df2 = b.dfs[tablename]
            tablelist.append(df2)

        if '2011' in table_map[tablename]:
            c = df_build(path3, 'range2011-2016')
            c.extract_fields('2009',tablename)
            c.append_fields('2011',tablename)
            df3 = c.dfs[tablename]
            tablelist.append(df3)

        if '2013' in table_map[tablename]:
            d = df_build(path4, 'range2011-2016')
            d.extract_fields('2009',tablename)
            d.append_fields('pasture2013',tablename)
            df4 = d.dfs[tablename]
            tablelist.append(df4)

        if '2017' in table_map[tablename]:
            e = df_build(path5, 'rangepasture2017_2018')
            e.extract_fields('2018',tablename)
            e.append_fields('rangepasture2017',tablename)
            df5 = e.dfs[tablename]
            tablelist.append(df5)
    if 'pastureheights' not in tablename:
        main_df = pd.concat([i for i in tablelist]).drop_duplicates()
    else:
        height = pd.concat([df2,df3,df4]).drop_duplicates().copy()
        height2 = pd.concat([height,df5],ignore_index=True)

    if 'concern' in tablename:
        main_df = concern(main_df)

    if 'disturbance' in tablename:
        main_df = disturbance(main_df)

    if 'pastureheights' in tablename:
        main_df = pastureheights(height2)

    if 'soilhorizon' in tablename:
        main_df = soilhorizon(main_df)

    if 'pintercept' in tablename:
        main_df = pintercept(main_df)

    if 'practice' in tablename:
        main_df = practice(main_df)



















    return main_df
