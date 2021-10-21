#
from src.utils.tools import db
from src.projects.dima.dima_handler import main_translate, table_collector, \
table_check, looper, batch_looper
from src.projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field, openingsize_fixer, datetime_type_assert
from src.projects.tall_tables.talltables_handler import ingesterv2

from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tables.lpipk import lpi_pk
from src.projects.dima.tabletools import fix_fields

from src.projects.dima.tables.bsnepk import bsne_pk
from src.projects.dima.tables.lpipk import lpi_pk
from src.projects.dima.tables.nopk import no_pk
from src.projects.dima.tables.gappk import gap_pk
from src.projects.dima.tables.sperichpk import sperich_pk
from src.projects.dima.tables.plantdenpk import plantden_pk
from src.projects.dima.tables.qualpk import qual_pk
import os
dir = r"C:\Users\kbonefont\Documents\GitHub\ingester_v3\dimas"
dimafiles = [os.path.normpath(f"{dir}/{i}") for i in os.listdir(dir)]

df = looper(dir,"tblLines", "Test")

switcher = {
    'tblPlots':no_pk,
    'tblLines':no_pk,
    'tblLPIDetail':lpi_pk ,
    'tblLPIHeader':lpi_pk ,
    'tblGapDetail':gap_pk ,
    'tblGapHeader':gap_pk ,
    'tblQualHeader':qual_pk ,
    'tblQualDetail':qual_pk ,
    'tblSoilStabHeader':no_pk ,
    'tblSoilStabDetail':no_pk ,
    'tblSoilPitHorizons':no_pk ,
    'tblSoilPits':no_pk ,
    'tblSpecRichHeader':sperich_pk ,
    'tblSpecRichDetail':sperich_pk ,
    'tblPlantProdHeader':no_pk,
    'tblPlantProdDetail':no_pk,
    'tblPlotNotes':no_pk,
    'tblPlantDenHeader':no_pk ,
    'tblPlantDenDetail':no_pk ,
    'tblSpecies':no_pk,
    'tblSpeciesGeneric':no_pk,
    'tblSites':no_pk,
    'tblBSNE_Box':bsne_pk ,
    'tblBSNE_BoxCollection':bsne_pk ,
    'tblBSNE_Stack':bsne_pk ,
    'tblBSNE_TrapCollection':bsne_pk
}

path= r"C:\Users\kbonefont\Documents\GitHub\ingesterv2\dimas\REPORT 16Apr18 El Reno DIMA 5.5a as of 2020-06-26.mdb"

single_mdb = r"" # absolute path to single mdb
mdb_dir = r"" # absolute path to directory for a batch of mdb's
d = db("chris") # requires 'database.ini' file to have postgres credentals

# checking tables for single mdb

table_collector(mdb_dir)

import psycopg2 as pg
import pandas.io.sql as psql

df = psql.read_sql("select * from public.\"dataHeader\" LIMIT 4", d.str)
df




# arc.MakeTableView("tblGapHeader",p3)

# create a single dataframe for a single table from a single MDB
head = main_translate('tblGapDetail',p3) # assigining dataframe to variable df

#create a single dataframe for a single table from ALL mdbs in a directory

gapdet = looper(mdb_dir,'tblGapDetail', csv=False)

gaphead = looper(mdb_dir,'tblGapHeader', csv=False)

 # assigning dataframe to variable df2

 # to check dataframe size
table_create(df,'tblGapDetail', "dima")
ingesterv2.main_ingest(df, "tblGapDetail", d.str, 100000) # sending dataframe to postgres
batch_looper(mdb_dir)


"tblPlantDenHeader",
"tblPlantDenDetail",
"tblPlantProdHeader",
"tblPlantProdDetail",
"tblSpecRichHeader",
"tblSpecRichDetail",
"tblSoilPits",
"tblSoilPitHorizons",
"tblSoilStabHeader",
"tblSoilStabDetail"



no_primary_key = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric',\
                  'tblSites','tblPlotNotes', 'tblSites']
soil_stab_primary_key = ['tblSoilStabDetail', 'tblSoilStabHeader']
soil_pit_primary_key = ['tblSoilPits','tblSoilPitHorizons']
plant_prod_primary_key = ['tblPlantProdDetail', 'tblPlantProdHeader']
plant_den_primary_key = ['tblPlantDenDetail', 'tblPlantDenHeader']
bsne_primary_keys = ['tblBSNE_Box', 'tblBSNE_Stack','tblBSNE_BoxCollection',\
                     'tblBSNE_TrapCollection']
sperich_primary_keys = ["tblSpecRichHeader", "tblSpecRichDetail"]

switcher_arguments= {
    'no_pk': (None, dimapath, tablename),
    'no_pk_soilstab': ('soilstab',dimapath, tablename),
    'no_pk_soilpits': ('soilpits',dimapath, tablename),
    'no_pk_plantprod': ('plantprod',dimapath, tablename),
    'no_pk_plantden': ('plantden',dimapath,tablename),
    'yes_pk': dimapath,
    'spe_rich_pk':(dimapath,tablename),
    'f': ('fake', dimapath, tablename)
    }
tablename = 'tblBSNE_Box'
from src.projects.dima.handler import switcher, tableswitch

arc = arcno(path)
arc.actual_list
arcno.MakeTableView("tblBSNE_Stack",path)
main_translate("tblBSNE_Stack", path)

table_check("tblBSNE_BoxCollectiond", path)
[True for i,j in inst.actual_list.items() if 'BSNE' in i]
df = switcher[tablename](*switcher_arguments['no_pk'])

df = switcher["tblBSNE_Box"](None, path, "tblBSNE_Box")
df = bsne_pk()
df = bsne_pk(path)

switcher[tablename](dimapath)

""""""""""""""""""""""""""""""""""""""""""
import pandas as pd
from src.utils.tools import db
from src.utils.arcnah import arcno
import os
from psycopg2 import sql
import numpy as np
from src.projects.dima.tablefields import tablefields

from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tabletools import fix_fields
import platform

dimapath =  r"C:\Users\kbonefont\Desktop\REPORT 16Apr18 El Reno DIMA 5.5a as of 2020-06-26.mdb"
arc = arcno(dimapath)
arc.actual_list

box = arcno.MakeTableView("tblBSNE_Box",dimapath)
stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)
# differences 1


pd.merge(box,stack, how="inner", on="StackID").sort_values(by="Location").iloc[:,5:]


df2 = pd.merge(df,boxcol, how="inner", on="BoxID")
df2.collectDate = pd.to_datetime(df2.collectDate) if platform.system()=='Linux' else df2.collectDate

# # fix
df2 = arc.CalculateField(df2,"PrimaryKey","PlotKey","collectDate")
df2tmp = fix_fields(df2,"Notes")

df = df2
keyword = "DateModified"
fix_fields(df,keyword)
df[["DateModified_x","DateModified_y"]]
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
                    df.drop(['Notes_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    return df

            else:
                return df
##############################################################
# reading pg tables as pandas
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd


dh = pd.read_sql_query('select * from "dataHeader" limit 1', engine_conn_string("postgresql"))
gs = pd.read_sql_query('select * from "dataSpeciesInventory" limit 1', engine_conn_string("postgresql"))
geoi = pd.read_sql_query('select * from "geoIndicators"', engine_conn_string("postgresql"))

[i for i in dh.columns if i not in gs.columns]

#
geoh = gpd.GeoDataFrame.from_postgis('select * from "dataHeader"', engine_conn_string("postgresql"), geom_col='wkb_geometry')
geo_mlra = gpd.GeoDataFrame.from_postgis('select * from gis."mlra_v42_wgs84"', engine_conn_string("postgresql"), geom_col='geom')
geo_mlra.dtypes
h_m = gpd.sjoin(geo_mlra,geoh, how="inner", op="intersects")

h_m.loc[:,["PrimaryKey","mlra_name"]]
# creating new columns from header using the ecoregions, mlra
 # import pandas as pd
 # from src.utils.tools import db
 # from src.utils.arcnah import arcno
 # import os
 # from psycopg2 import sql
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
from base64 import b64decode
import ogr
import binascii
from shapely import wkb

v
dh = pd.read_sql_query('select * from "dataHeader"', engine_conn_string("postgresql"))
mlra = pd.read_sql_query('select * from gis."mlra_v42_wgs84"', engine_conn_string("postgresql"))

returnFields("ecolevel1")

returnFields("ecolevel2")

returnFields("ecolevel3")

returnFields("ecolevel4")

all = returnFields("mlra")
[i for i in geoi.columns]
geoi.PrimaryKey
df = geoi.copy()
def returnFields(which_map, df = None):
    """
    if supplied with dataframe, it will return a dataframe
    with the additional fields: na_l1name, na_l2name, us_l3name, us_l4name
    and mlra. these fields are produced by an initial join between any table
    with header to get PrimaryKeys on every row, and an intersect spatial join
    with ecoregion or mlra to find in which spatial category each
    PK+point geometry row falls within

    if df = None,

    """
    maps = {
        "mlra":"mlra_v42_wgs84",
        "ecolevel1":"us_eco_level_4",
        "ecolevel2":"us_eco_level_4",
        "ecolevel3":"us_eco_level_4",
        "ecolevel4":"us_eco_level_4",
        "ecolevels":"us_eco_level_4"
    }
    which_field= {
        "mlra":"mlra_name",
        "ecolevel1":"na_l1name",
        "ecolevel2":"na_l2name",
        "ecolevel3":"us_l3name",
        "ecolevel4":"us_l4name",
        "ecolevels":""
    }
    result_set = {
    "mlra":["PrimaryKey", which_field[which_map]],
    "ecolevel1":["PrimaryKey",which_field[which_map]],
    "ecolevel2":["PrimaryKey",which_field[which_map]],
    "ecolevel3":["PrimaryKey",which_field[which_map]],
    "ecolevel4":["PrimaryKey",which_field[which_map]],
    "ecolevels":["PrimaryKey",which_field["ecolevel1"],which_field["ecolevel2"],which_field["ecolevel3"],which_field["ecolevel4"]]
    }
    if df is None:
        poly = gpd.GeoDataFrame.from_postgis(f'select * from gis.{maps[which_map]}', engine_conn_string("postgresql"), geom_col='geom')
        points = gpd.GeoDataFrame.from_postgis('select * from "dataHeader"', engine_conn_string("postgresql"), geom_col='wkb_geometry')
        join = gpd.sjoin(poly,points, how="inner", op="intersects")
        return join.loc[:,result_set[which_map]]
    # elif df is not None and isinstance(df,pd.DataFrame):
    #     # print("ok")
    #     poly = gpd.GeoDataFrame.from_postgis(f'select * from gis.{maps[which_map]}', engine_conn_string("postgresql"), geom_col='geom')
    #     join = pd
dftype = type(df)

geoi.merge(all,how="inner",on="PrimaryKey")
type(dftype)
isinstance(df,pd.DataFrame)
which_map = "ecolevels"

result_set["ecolevels"]


[i for i in maps.keys() if 'mlra' not in i]

b = dh["wkb_geometry"][0]
format(wkb,'b')
h = ogr.CreateGeometryFrom(dh["wkb_geometry"][0])
h = wkb.loads(b, hex=True)

h
# dataframe still has well known binary instead of points
dh
pd.read_sql_query('select wkb_geometry from "dataHeader" limit 1',engine_conn_string("postgresql"))
binar=format(dh["wkb_geometry"][0],'b')
wkb = dh["wkb_geometry"][0]





df = pd.read_sql_query(f'select "mlra_name" from gis."mlra_v42_wgs84" where ST_WITHIN(ST_GeomFromText(\'{h}\',4326)::geometry,geom);', engine_conn_string("postgresql"))
df.loc[0,"mlra_name"]
def mlraCreate(wellknown):

    decode = wkb.loads(wellknown,hex=True)
    tmp = pd.read_sql_query(f'select "mlra_name" from gis."mlra_v42_wgs84" where ST_WITHIN(ST_GeomFromText(\'{decode}\',4326)::geometry,geom);', engine_conn_string("postgresql"))

    return df.loc[0,'mlra_name']

dh['mlra'] = dh.wkb_geometry.apply(lambda x: mlraCreate(x))

all
p = r"C:\Users\kbonefont\Desktop\new_data_tall\gap_tall.csv"
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
from base64 import b64decode
import ogr
import binascii
from shapely import wkb
import requests
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd

df = pd.read_csv(p, low_memory=False)
df.iloc[:5,:]
pd.merge(df1, all, how="inner",on=["PrimaryKey"])
df1 = df.iloc[:5,:].copy(deep=True)
df1.info()
df1.to_json()

headers = {'content-type':'application/json'}
df.iloc[:2,144:]
# options
df
df_json = df.to_json(orient='records')
s = requests.Session()
reqObj = {
    "data":df_json
}
s.post('http://localhost:3001/o',
        headers=headers,
        data=df_json,
        stream=True)



import pandas as pd
r = r"C:\Users\kbonefont\Documents\modis_classes.csv"

eng = engine_conn_string("postgresql")


df = pd.read_csv(r)
df.to_sql('modis_classes', eng)


tmp = gpd.GeoDataFrame.from_postgis('select mlrarsym, mlra_name, geom from gis.mlra_v42_wgs84',
                engine_conn_string("postgresql"),
                geom_col='geom')

p = r"C:\Users\kbonefont\Desktop\geo_files\geoInd.csv"
df = pd.read_csv(p, encoding='cp1252',low_memory=False)
df = df.loc[:5,:]

# [row for row in df.itertuples(index=False)]
# [i for i in df.to_dict(orient='records')[0].keys()]
# for i in df.itertuples(index=False):
#     lst = [i[j] for j in range(0,len(df.columns))]
#     print( f'Values {*lst,}' )
# [[i[j] for j in range(0,len(df.columns))] for i in df.itertuples(index=False)]

def geoindicatorsHandler(df):
    """
    sends and array with all the geometries into
    a udf on postgis,
    creates an updated dataframe

    """
import numpy as np
[['NULL' if type(y)==float and np.isnan(y) else y for y in i] for i in df.itertuples(index=False) ]
print(' ,'.join([i for i in df.columns]))
spatiallyExplicit(df)
def spatiallyExplicit(df):
    # list of all columns available
    strCols = ' ,'.join([f'"{i}"' for i in df.columns])
    # creating empty df
    df_empty = df[0:0]
    df_empty['wkb_geometry'] = pd.NA
    df_geom = gpd.GeoDataFrame(df_empty,geometry="wkb_geometry")
    # row wise join of geometries to geoind
    for i in df.itertuples(index=False):
        try:
            # https://stackoverflow.com/a/25096109
            lst = [['NULL' if type(y)==float and np.isnan(y) else y for y in i][j] for j in range(0,len(df.columns))]
            vals = f'Values {*lst,}'
            sql_join = f"""
                select dh.wkb_geometry,dh."PrimaryKey" from public."dataHeader" as dh    JOIN ( {vals} )
                as t ({strCols})
                ON dh."PrimaryKey" = t."PrimaryKey";
                """
            row = gpd.GeoDataFrame.from_postgis(sql_join, eng,geom_col='wkb_geometry')
            row_merged= pd.merge(df, row.loc[:,["wkb_geometry","PrimaryKey"]], how="inner", on="PrimaryKey")
            df_geom = df_geom.append(row_merged)
            # df_geom = pd.concat([df_geom,row_merged], ignore_index=True)
        except Exception as e:
            print(e)
    return df_geom
df1 = spex(df)
# df1,

def spex(df):
    geoms = gpd.GeoDataFrame.from_postgis("select \"PrimaryKey\",wkb_geometry from public.\"dataHeader\";",eng, geom_col="wkb_geometry")
    fin = geoms.merge(df, on="PrimaryKey")
    return fin
# m = mlra()
def mlra():
    """
    utility function to bring mlra table for a geopandas join

    """
    try:
        tmp = gpd.GeoDataFrame.from_postgis('select mlrarsym, mlra_name, geom from gis.mlra_v42_wgs84',
                engine_conn_string("postgresql"),
                geom_col='geom')
        return tmp
    except Exception as e:
        print(e)
cols = [i if i not in ['wkb_geometry'] else i for i in df1.columns ]
cols.remove(['PrimaryKey'])
cols.extend(['mlrarsym','mlra_name'])
test = gpd.sjoin(df1,m, op="intersects")

test.loc[:,cols]
jn(df1,m,df)
def jn(target,source, colsdf):
    cols = [i for i in colsdf.columns]
    cols.extend(['mlrarsym','mlra_name'])
    final = gpd.sjoin(df1,m, op="intersects")
    return final.loc[:,cols]


lst = [i[j] for j in range(0,len(df.columns))]
vals = f"VALUES { *lst, }"

sql_join = f"""
    select dh.wkb_geometry,dh."PrimaryKey" from public."dataHeader" as dh    JOIN ( {vals} )
    as t ({strCols})
    ON dh."PrimaryKey" = t."PrimaryKey";
    """
print(sql_join)
row = gpd.GeoDataFrame.from_postgis(sql_join, eng,geom_col='wkb_geometry')
row.loc[:,["wkb_geometry","PrimaryKey"]]
df.PrimaryKey
row.info()
row1 = pd.merge(df, row.loc[:,["wkb_geometry","PrimaryKey"]], how="inner", on="PrimaryKey")
df_clear = df[0:0]
df_clear['wkb_geometry'] = pd.NA
clear_geom = gpd.GeoDataFrame(df_clear,geometry="wkb_geometry")
clear_geom.append(row1)



"""
the alaska debacle
"""

# took 26 secs
us = gpd.GeoDataFrame.from_postgis('select * from gis.us_eco_level_4',
                                    engine_conn_string("postgresql"),
                                    geom_col='geom')
# took 1.9 secs
ak = gpd.GeoDataFrame.from_postgis('select * from gis.ak_ecoregions',
                                    engine_conn_string("postgresql"),
                                    geom_col='geom')

us.columns
len(ak.columns)
re1 = us.rename(columns={''})
us_filtered = filter(lambda field: 'key' not in field, us.columns)
ak_filtered = filter(lambda field: 'key' not in field, ak.columns)
us.loc[:,[i if 'key' not in i else None for i in us.columns]]
print(list(filter))
list(filtered)
ak.loc[:,list(ak_filtered)]
merged = us.append(ak)
merged.loc[:,['us_l4name', 'us_l3name', 'na_l2name', 'na_l1name', 'geom']]
""""""

# test with bare geoindicators
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
from base64 import b64decode
import ogr
import binascii
from shapely import wkb
import requests
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import logging
# loading geoind
p = r"C:\Users\kbonefont\Desktop\geo_files\geoInd.csv"
df = pd.read_csv(p, encoding='cp1252',low_memory=False)

def mlra()-> gpd.GeoDataFrame:
    """ queries the mlra table into a geopandas dataframe
    """
    try:
        tmp = gpd.GeoDataFrame.from_postgis('select mlrarsym, mlra_name, geom from gis.mlra_v42_wgs84',
                engine_conn_string("postgresql"),
                geom_col='geom')
        return tmp
    except Exception as e:
        logging.error(e)

mlra = mlra() # took 9s
spatialdf1 = header_pk_geometry(df) # took 2s
mlra_geoind = geoindicators_mlra(spatialdf1,mlra,df) # took 17s
eco = ecoregions() # took 40s
final = geoindicators_ecoregions(mlra_geoind,eco, df)

def header_pk_geometry(dataframe:pd.DataFrame) -> gpd.GeoDataFrame :
    """ spatial join between geoindicators and select fields in
    dataheader(primary key and wkb_geometry) to make geoindicators
    spatially explicit.

    PARAMS:
    dataframe: pandas dataframe. original unmodified dataframe
    """
    try:
        geoms = gpd.GeoDataFrame.from_postgis("select \"PrimaryKey\",wkb_geometry from public.\"dataHeader\";",
        engine_conn_string("postgresql"),
        geom_col="wkb_geometry")
        fin = geoms.merge(dataframe, on="PrimaryKey")
        logging.info('Returning a merge between header and supplied dataframe')
        return fin
    except Exception as e:
        logging.error(e)



def geoindicators_mlra(
    spatial_geoindicators:gpd.GeoDataFrame,
    mlra_df:gpd.GeoDataFrame,
    columns_df:pd.DataFrame) -> gpd.GeoDataFrame:
    """ spatial join between spatially explicit geoindicators
    and mlra. returns geoindicators + mlrarsym and mlra_name fields
    requires geoIndicators primary keys to be already present
    on the DB.

    PARAMS:
    spatial_geoindicators: geoPandas dataframe. product of header_pk_geometry()
    mlra_df: geoPandas dataframe. product of mlra()
    columns_df: pandas dataframe. original unmodified dataframe
    """
    cols = [i for i in columns_df.columns]
    cols.extend(['wkb_geometry','mlrarsym','mlra_name'])
    final = gpd.sjoin(spatial_geoindicators, mlra_df, op="intersects")
    logging.info("Returning a spatial join between mlra and spatially-explicit geoindicators")
    return final.loc[:,cols]

def ecoregions():
    try:
        us = gpd.GeoDataFrame.from_postgis('select * from gis.us_eco_level_4',
                                    engine_conn_string("postgresql"),
                                    geom_col='geom')

        ak = gpd.GeoDataFrame.from_postgis('select * from gis.ak_ecoregions',
                                    engine_conn_string("postgresql"),
                                    geom_col='geom')
        merged = us.append(ak)
        return merged.loc[:,['us_l4name', 'us_l3name', 'na_l2name', 'na_l1name', 'geom']]

    except Exception as e:
        logging.error(e)

def geoindicators_ecoregions(
    spatial_geoindicators:gpd.GeoDataFrame,
    ecoregions_df:gpd.GeoDataFrame,
    columns_df:pd.DataFrame) -> gpd.GeoDataFrame:

    cols = [i for i in columns_df.columns]
    cols.extend(['mlrarsym','mlra_name','us_l4name', 'us_l3name', 'na_l2name', 'na_l1name'])
    final = gpd.sjoin(spatial_geoindicators, ecoregions_df, op="intersects")
    logging.info("Returning a spatial join between ecoregions and spatially-explicit geoindicators")
    return final.loc[:,cols]

mlra_geoind.wkb_geometry
gpd.sjoin(mlra_geoind,eco,op="intersects")

"""
raster deals
"""

from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
from base64 import b64decode
import ogr
import binascii
from shapely import wkb
import requests
from src.projects.project import engine_conn_string
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import logging

rstr= """
    SELECT x, y, val, geom
    FROM
        (
        SELECT db.* FROM modis_merge,
        LATERAL ST_PixelAsCentroids(rast,1) AS dp) foo
        )

"""
import xarray as xr
import rasterio
p = r"C:\Users\kbonefont\Documents\GitHub\gdal_COG\io\modismerge\modis_mergeCOG.tif"

xr.open_rasterio(p)

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio

with rio.Env():
    with rio.open('https://github.com/OSGeo/gdal/raw/master/autotest/gdrivers/data/float32.tif') as src:
        crs = src.crs

        # create 1D coordinate arrays (coordinates of the pixel center)
        xmin, ymax = np.around(src.xy(0.00, 0.00), 9)  # src.xy(0, 0)
        xmax, ymin = np.around(src.xy(src.height-1, src.width-1), 9)  # src.xy(src.width-1, src.height-1)
        x = np.linspace(xmin, xmax, src.width)
        y = np.linspace(ymax, ymin, src.height)  # max -> min so coords are top -> bottom



        # create 2D arrays
        xs, ys = np.meshgrid(x, y)
        zs = src.read(1)

        # Apply NoData mask
        mask = src.read_masks(1) > 0
        xs, ys, zs = xs[mask], ys[mask], zs[mask]

data = {"X": pd.Series(xs.ravel()),
        "Y": pd.Series(ys.ravel()),
        "Z": pd.Series(zs.ravel())}

df = pd.DataFrame(data=data)
geometry = gpd.points_from_xy(df.X, df.Y)
gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

print(gdf.head())
#?
from src.projects.project import engine_conn_string
import pandas as pd
eng = engine_conn_string("mainapi")
df = pd.read_sql("select * from public.\"dataHeader\" limit 5", eng);


















#
