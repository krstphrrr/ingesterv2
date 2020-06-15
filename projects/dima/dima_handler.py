from utils.arcnah import arcno
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd
from projects.tables.handler import no_pk, lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk, switcher, tableswitch, fix_fields


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
        'e':dimapath,
        'f':('fake', dimapath, tablename)
    }
    if tablename in a:
        network_check = 0
        inst = arcno(dimapath)

        for i,j in inst.actual_list.items():
            if any([True for i,j in inst.actual_list.items() if 'BSNE' in i]):
                network_check = 2
            else:
                network_check = 1
        while network_check!=0:
            print(network_check)
            if network_check==1:
                df = switcher[tablename](*types['f'])
                network_check=0
                return df

            elif network_check==2:
                df = switcher[tablename](*types['a'])
                network_check=0
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

def pg_send(tablename,dimapath):
    plot = None
    """
    almost done:
    - add DBKey
    - create Horizontafllux and dust

    """









            # modcheck+=1
