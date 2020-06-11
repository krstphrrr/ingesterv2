from utils.arcnah import arcno
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd
from projects.tables.handler import no_pk, lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk



"""
    elif tablename in linekeylist:
        if tablename.find('Gap')!=-1:
            fulldf = gap_pk(dimapath)
            arc.isolateFields(fulldf, 'LineKey','PrimaryKey')
            line_tmp = arc.isolates
            lin = line_tmp.rename(columns={'LineKey':'LineKey2'}).copy(deep=True)
            lin = lin.drop_duplicates(['LineKey2'])
            linekeytable = arc.MakeTableView(f'{tablename}',dimapath)
            linekeytable_pk = lin.merge(linekeytable, left_on=lin.LineKey2, right_on=linekeytable.LineKey)
            linekeytable_pk.drop('LineKey2', axis=1, inplace=True)
            linekeytabler_pk = linekeytable_pk.copy(deep=True)
            linekeytable_pk.drop('key_0', axis=1, inplace=True)
            # mdb[f'{tablename}'] = linekeytable_pk
            return linekeytable_pk
"""

fulldf = gap_pk(path3)

for i in arc.actual_list:
    print(i)


for i in arc.maintablelist:
    if arc.MakeTableView(i, path).shape[0]>1:
        print(i)

arc.actual_list


"""
from path to ingest:

1. path and table name into  'pg_send'
2. if 'pg_send' finds 'BSNE_Box', 'BSNE_BoxCollection' and 'BSNE_TrapCollection' ==>
 - bsne_pk : adds primary key to lowerlevel, joins and propagates pk upstream

   if NOT ==>
 - pk_add : regular table name and path
"""



def isolate(tabletype,path):
    tableswitch ={
        'lpi':lpi_pk(path),
        'gap':gap_pk(path),
        'sperich':sperich_pk(path),
        'plantden':plantden_pk(path),
        'bsne':bsne_pk(path)
    }
    arc = arcno()
    fulldf = tableswitch[tabletype]

arcno.MakeTableView('tblPlotNotes', path10).columns
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

    # if tablename in plotkeylist:
test  = arcno.MakeTableView('tblPlantProdDetail', path10)
testt = arcno.MakeTableView('tblPlantProdHeader', path10)

head_det = pd.merge(test,testt, how="inner", on="RecKey")
no_pk('plantprod', path10)

def has_plotkey_andformdate(dimapath):
    """
    {'tblGapDetail': 'rows: 2350',
     'tblGapHeader': 'rows: 180',
     'tblLines': 'rows: 181',
     'tblLPIDetail': 'rows: 18000',
     'tblLPIHeader': 'rows: 180',
     'tblPlantProdDetail': 'rows: 335',
     'tblPlantProdHeader': 'rows: 27',
     'tblPlotNotes': 'rows: 41',
     'tblPlots': 'rows: 61',
     'tblSites': 'rows: 14',
     'tblSoilPitHorizons': 'rows: 208',
     'tblSoilPits': 'rows: 60',
     'tblSoilStabDetail': 'rows: 180',
     'tblSoilStabHeader': 'rows: 60',
     'tblSpecies': 'rows: 6073',
     'tblSpeciesGeneric': 'rows: 1082',
     'tblSpecRichDetail': 'rows: 61',
     'tblSpecRichHeader': 'rows: 60'}

    """
    arc = arcno()
    df = arcno.MakeTableView(dimapath)
def joinfun(df,whichfield):
    arc = arcno()
    # df=df.copy()
    arc.isolateFields(df, f'{whichfield}', 'PrimaryKey')
    first = arc.isolates.copy()
    first.rename(columns={f'{whichfield}':f'{whichfield}2'}, inplace=True)
    # first=first.drop_duplicates(['PlotKey2'])
    return first
def pk_yesno(tablename,dimapath):
    nopk = ['tblPlots','tblLines','tblSpecies', 'tblSpecies', 'tblSpeciesGeneric']
    if tablename in nopk:
        df = arcno.MakeTableView(tablename,dimapath)
        return df
    else:
        if "tblGap" in tablename:
            fulldf = gap_pk(tablename)


if join_back(tablename, dimapath):
    pass
arc = arcno(path1,False)
arc.tablelist
arc.actual_list

def main_translate(tablename,dimapath):
    arc = arcno()
    tableswitch ={
        'tblPlots':"no pk",
        'tblLines':"no pk",
        'tblLPIDetail':"RecKey",
        'tblLPIHeader':"LineKey",
        'tblGapDetail':"RecKey",
        'tblGapHeader':"LineKey",
        # 'tblQualHeader':no_pk(dimapath,),
        # 'tblQualDetail':no_pk(dimapath),
        'tblSoilStabHeader':"PlotKey",
        'tblSoilStabDetail':"RecKey",
        'tblSoilPitHorizons':"no pk",
        'tblSoilPits':"no pk",
        'tblSpecRichHeader':"LineKey",
        'tblSpecRichDetail':"RecKey",
        'tblPlantProdHeader':"PlotKey",
        'tblPlantProdDetail':"RecKey",
        'tblPlotNotes':"no pk",
        'tblPlantDenHeader':"LineKey",
        'tblPlantDenDetail':"RecKey",
        'tblSpecies':"no pk",
        'tblSpeciesGeneric':"no pk",
        'tblSites':"no pk",
        'tblBSNE_Box':"BoxID",
        'tblBSNE_BoxCollection':"BoxID",
        'tblBSNE_Stack':"PlotKey",
        'tblBSNE_TrapCollection':"StackID"
    }
    translate_table = {
        'tblPlots':no_pk(tablefam=None,dimapath=dimapath,tablename=tablename),
        'tblLines':no_pk(tablefam=None,dimapath=dimapath,tablename=tablename),
        'tblLPIDetail':lpi_pk(dimapath),
        'tblLPIHeader':lpi_pk(dimapath),
        'tblGapDetail':gap_pk(dimapath),
        'tblGapHeader':gap_pk(dimapath),
        # 'tblQualHeader':no_pk(dimapath),
        # 'tblQualDetail':no_pk(dimapath),
        'tblSoilStabHeader':no_pk('soilstab',dimapath,None),
        'tblSoilStabDetail':no_pk('soilstab',dimapath,None),
        # 'tblSoilPitHorizons',
        # 'tblSoilPits',
        'tblSpecRichHeader':sperich_pk(dimapath),
        'tblSpecRichDetail':sperich_pk(dimapath),
        'tblPlantProdHeader':no_pk('plantprod',dimapath,None),
        'tblPlantProdDetail':no_pk('plantprod',dimapath,None),
        # 'tblPlotNotes',
        'tblPlantDenHeader':plantden_pk(dimapath),
        'tblPlantDenDetail':plantden_pk(dimapath),
        'tblSpecies':no_pk(tablefam=None,dimapath=dimapath,tablename=tablename),
        'tblSpeciesGeneric':no_pk(tablefam=None,dimapath=dimapath,tablename=tablename),
        'tblSites':no_pk(tablefam=None,dimapath=dimapath,tablename=tablename),
        'tblBSNE_Box':bsne_pk(dimapath),
        'tblBSNE_BoxCollection':bsne_pk(dimapath),
        'tblBSNE_Stack':bsne_pk(dimapath),
        'tblBSNE_TrapCollection':bsne_pk(dimapath)
    }

    full = translate_table[tablename]
    if tableswitch[tablename]=="no pk":
        return full
    else:
        iso = arc.isolateFields(full,tableswitch[tablename],"PrimaryKey").copy()
        iso.drop_duplicates([tableswitch[tablename]],inplace=True)

        target_table = arcno.MakeTableView(tablename, dimapath)
        retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
        del(arc)
        return retdf


main_translate('tblSpecies',path1)






















            # modcheck+=1
