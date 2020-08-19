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
from projects.dima.tabletools import fix_fields, new_tablename, table_create, \
tablecheck, csv_fieldcheck, blank_fixer, significant_digits_fix_pandas, \
float_field
from projects.tall_tables.talltables_handler import ingesterv2


def main_translate(tablename:str, dimapath:str, debug=None):
    """ Translates between tables and different function argument strategies

    This function will map the tablename to an appropriate tuple of arguments.
    first block of 'a' argument tuple will check if the dima has 'BSNE' tables
    within the DIMA. It could still be a vegetation dima however, that will be checked
    further along on the no_pk function.

    Parameters
    ----------
    tablename : str
        Name of the table in DIMA. example: 'tblLines'

    dimapath : str
        Physical path to DIMA Access file. example: 'c://Folder/dima_file.mdb'

    debug : any character or None (default value: None)
        Prints out how trayectory of the mapping process throughout the function.

    """

    a = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric','tblSites','tblPlotNotes', 'tblSites']
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
                df = blank_fixer(df)
                df = significant_digits_fix_pandas(df)
                return df

            elif network_check==2:
                print('no_pk; netdima in path; line or plot') if debug else None
                df = switcher[tablename](*types['a'])
                network_check=0
                df = blank_fixer(df)
                df = significant_digits_fix_pandas(df)
                return df

    elif tablename in b:
        # no_pk + soilstab branch
        print('no_pk; soilstab') if debug else None
        df = switcher[tablename](*types['b'])
        df = blank_fixer(df)
        df = significant_digits_fix_pandas(df)
        return df

    elif tablename in c:
        # no_pk + soilpits branch
        print('no_pk; soilpits') if debug else None
        df = switcher[tablename](*types['c'])
        df = blank_fixer(df)
        df = significant_digits_fix_pandas(df)
        return df

    elif tablename in d:
        # no_pk + plantprod branch
        print('no_pk; plantprod') if debug else None
        df = switcher[tablename](*types['d'])
        df = blank_fixer(df)
        df = significant_digits_fix_pandas(df)
        return df

    else:
        # lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk branch
        if tablename in e:
            print('bsne collection') if debug else None
            retdf = switcher[tablename](types['e'])
            retdf = blank_fixer(retdf)
            retdf = significant_digits_fix_pandas(retdf)
            retdf.openingSize = float_field(retdf, 'openingSize')
            return retdf
        else:
            print('hmmm?') if debug else None
            df = switcher[tablename](types['e'])
            arc = arcno()
            iso = arc.isolateFields(df,tableswitch[tablename],"PrimaryKey").copy()
            iso.drop_duplicates([tableswitch[tablename]],inplace=True)

            target_table = arcno.MakeTableView(tablename, dimapath)
            retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
            retdf = blank_fixer(retdf)
            retdf = significant_digits_fix_pandas(retdf)
            retdf.openingSize = float_field(retdf, 'openingSize')
            return retdf


def pg_send(table:str, path:str, csv=None, debug=None):
    plot = None
    """ Sends dataframe to postgres or prints out CSV.

    Given a Dima path and tablename, uses the function main_translate to create
    a dataframe and either send it to a postgres database, or print it out to
    a CSV in the same directory where the DIMA file is in.

    Parameters
    ----------

    table : str
        Name of the table in DIMA. example: 'tblLines'

    path : str
        Physical path to DIMA Access file. example: 'c://Folder/dima_file.mdb'

    csv : None or any character. (default value: None)
        If not None, it will print out the table dataframe to CSV.

    debug : None or any character. (default value: None)
        If not None, it will print out each of the steps the function's processes
        take.
    """
    d = db('dima')
    df = main_translate(table,path)
    print("STARTING INGEST")
    df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # dbkey add calibration HERE
    if ('calibration' in path) or ('Calibration' in path):
            df['DBKey'] = os.path.join(split(splitext(path)[0])[1].replace(" ",""),'calibration')
    else:
        # creates non-calibration DBKey here
        df['DBKey'] = split(splitext(path)[0])[1].replace(" ","")

    if 'ItemType' in df.columns:
        # if one of the non-vegetation bsne tables, use 'new_tablename' ,
        # function to produce a new tablename: 'tblHorizontalFlux' or
        # 'tblDustDeposition'
        newtablename = new_tablename(df)
        if tablecheck(newtablename):
            print('MWACK')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)
        else:
            table_create(df, newtablename, 'dima')
            print('llegue a 2')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)

    else:
        print("NOT A HORFLUX TABLE")
        newtablename = table
        if tablecheck(table):
            print("FOUND THE TABLE IN PG")
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)

        else:
            print("DID NOT FIND TABLE IN PG, CREATING...")
            table_create(df, table, 'dima')
            ingesterv2.main_ingest(df, newtablename, d.str, 10000) if csv else csv_fieldcheck(df,path,table)

def looper(path2mdbs, tablename, csv=False):
    """
    creates fully concatenated table of multiple dimas in a single folder.
    either returns dataframe or returns csv.
    does not send to pg yet.
    """
    containing_folder = path2mdbs
    contained_files = os.listdir(containing_folder)
    df_dictionary={}

    count = 1
    basestring = 'file_'
    for i in contained_files:
        if os.path.splitext(os.path.join(containing_folder,i))[1]=='.mdb':
            countup = basestring+str(count)
            # df creation/manipulation starts here
            df = main_translate(tablename,os.path.join(containing_folder,i))
            df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            df['DBKey'] = os.path.split(os.path.splitext(i)[0])[1].replace(" ","")
            # df add to dictionary list
            df_dictionary[countup] = df.copy()
            count+=1
    final_df = pd.concat([j for i,j in df_dictionary.items()])
    return final_df if csv==False else final_df.to_csv(os.path.join(containing_folder,tablename+'.csv'))

            # modcheck+=1
