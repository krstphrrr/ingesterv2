from utils.arcnah import arcno
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd
from projects.tables.handler import no_pk, lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk, switcher, tableswitch

def main_translate(tablename,dimapath):

    a = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric','tblSites']
    b = ['tblSoilStabDetail', 'tblSoilStabHeader']
    c = ['tblSoilPits','tblSoilPitHorizons']
    d = ['tblPlantProdDetail', 'tblPlantProdHeader']

    types={
        'a':(None, dimapath, tablename),
        'b':('soilstab',dimapath, None),
        'c':('soilpits',dimapath, None),
        'd':('plantprod',dimapath, None),
        'e':dimapath
    }
    if tablename in a:
        df = switcher[tablename](*types['a'])
        return df

    elif tablename in b:
        df = switcher[tablename](*types['b'])
        return df

    elif tablename in c:
        df = switcher[tablename](*types['c'])
        return df

    elif tablename in d:
        df = switcher[tablename](*types['d'])
        return df
        
    else:
        df = switcher[tablename](types['e'])
        arc = arcno()
        iso = arc.isolateFields(df,tableswitch[tablename],"PrimaryKey").copy()
        iso.drop_duplicates([tableswitch[tablename]],inplace=True)

        target_table = arcno.MakeTableView(tablename, dimapath)
        retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
        return retdf


"""
from path to ingest:

1. path and table name into  'pg_send'
2. if 'pg_send' finds 'BSNE_Box', 'BSNE_BoxCollection' and 'BSNE_TrapCollection' ==>
 - bsne_pk : adds primary key to lowerlevel, joins and propagates pk upstream

   if NOT ==>
 - pk_add : regular table name and path
"""

def pg_send(tablename,dimapath):
    plot = None
    """
    build strategies:
    - plotkeylist
    """
    nopk = ['tblPlots','tblLines','tblSpecies', 'tblSpecies', 'tblSpeciesGeneric']

    plotkeylist = ['tblQualHeader','tblSoilStabHeader',
    'tblSoilPits','tblPlantProdHeader','tblPlotNotes', 'tblSoilPitHorizons'

    linekeylist = ['tblGapHeader','tblLPIHeader','tblSpecRichHeader',
    'tblPlantDenHeader']

    reckeylist = ['tblGapDetail','tblLPIDetail','tblQualDetail','tblSoilStabDetail',
    'tblSpecRichDetail','tblPlantProdDetail','tblPlantDenDetail']

    nonline = {'tblQualDetail':'tblQualHeader',
    'tblSoilStabDetail':'tblSoilStabHeader','tblPlantProdDetail':'tblPlantProdHeader'}



def joinfun(df,whichfield):
    arc = arcno()
    # df=df.copy()
    arc.isolateFields(df, f'{whichfield}', 'PrimaryKey')
    first = arc.isolates.copy()
    first.rename(columns={f'{whichfield}':f'{whichfield}2'}, inplace=True)
    # first=first.drop_duplicates(['PlotKey2'])
    return first






            # modcheck+=1
