from utils.arcnah import arcno
import os
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd

from projects.dima.handler import switcher, tableswitch
from projects.dima.tabletools import fix_fields, new_tablename, table_create,tablecheck
from projects.tall_tables.talltables_handler import ingesterv2


def main_translate(tablename,dimapath, debug=None):

    a = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric','tblSites','tblPlotNotes']
    b = ['tblSoilStabDetail', 'tblSoilStabHeader']
    c = ['tblSoilPits','tblSoilPitHorizons']
    d = ['tblPlantProdDetail', 'tblPlantProdHeader']
    e = ['tblBSNE_Box', 'tblBSNE_Stack','tblBSNE_BoxCollection', 'tblBSNE_TrapCollection']

    types={
        'a': (None, dimapath, tablename),
        'b': ('soilstab',dimapath, None),
        'c': ('soilpits',dimapath, None),
        'd': ('plantprod',dimapath, None),
        'e': dimapath,
        'f': ('fake', dimapath, tablename)
    }
    if tablename in a:
        # no_pk branch
        network_check = 0
        inst = arcno(dimapath)

        for i,j in inst.actual_list.items():
            if any([True for i,j in inst.actual_list.items() if 'BSNE' in i]):
                network_check = 2
            else:
                network_check = 1
        while network_check!=0:

            if network_check==1:
                print('no_pk; netdima in path; line or plot') if debug else None
                df = switcher[tablename](*types['f'])
                network_check=0
                return df

            elif network_check==2:
                print('no_pk; netdima in path; line or plot') if debug else None
                df = switcher[tablename](*types['a'])
                network_check=0
                return df

    elif tablename in b:
        # no_pk + soilstab branch
        print('no_pk; soilstab') if debug else None
        df = switcher[tablename](*types['b'])
        return df

    elif tablename in c:
        # no_pk + soilpits branch
        print('no_pk; soilpits') if debug else None
        df = switcher[tablename](*types['c'])
        return df

    elif tablename in d:
        # no_pk + plantprod branch
        print('no_pk; plantprod') if debug else None
        df = switcher[tablename](*types['d'])
        return df

    else:
        # lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk branch
        if tablename in e:
            print('bsne collection') if debug else None
            retdf = switcher[tablename](types['e'])
            return retdf
        else:
            print('hmmm?') if debug else None
            df = switcher[tablename](types['e'])
            arc = arcno()
            iso = arc.isolateFields(df,tableswitch[tablename],"PrimaryKey").copy()
            iso.drop_duplicates([tableswitch[tablename]],inplace=True)

            target_table = arcno.MakeTableView(tablename, dimapath)
            retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
            return retdf

def pg_send(table,path,csv=None):
    plot = None
    """
    almost done:
    - add DBKey
    - create Horizontafllux and dust

    """
    d = db('dima')
    df = main_translate(table,path)

    df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # dbkey add calibration HERE
    df['DBKey']=split(splitext(path)[0])[1].replace(" ","")

    if 'ItemType' in df.columns:
        newtablename = new_tablename(df)
        if tablecheck(newtablename):
            print('llegue aqui 1')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))
        else:
            table_create(df, newtablename)
            print('llegue a 2')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))

    else:
        if tablecheck(table):
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))

        else:
            table_create(df, table)
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else df.to_csv(os.path.join(os.path.dirname(path),table.replace('tbl','')+'.csv'))



            # modcheck+=1
