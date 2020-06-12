from utils.arcnah import arcno
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd
from projects.tables.handler import no_pk, lpi_pk, gap_pk, sperich_pk, plantden_pk, bsne_pk, switcher, tableswitch


path1 = r'C:\Users\kbonefont\Desktop\Network_DIMAs\8May2017 DIMA 5.5a as of 2020-03-10.mdb'
path2 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\21May2015 DIMA 5.5a as of 2020-03-10.mdb"
path3 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\REPORT 5May15 - 5Mar19 JER DIMA 5.4 as of 2019-04-19.mdb"
path4 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\REPORT 7Jun19 JER DIMA 5.4 as of 2019-04-19.mdb"
path5 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\REPORT 13Dec19 JER DIMA 5.4 as of 2019-04-19.mdb"
path6 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\REPORT 18Sept19 JER DIMA 5.4 as of 2019-04-19.mdb"
path7 = r"C:\Users\kbonefont\Desktop\Network_DIMAs\REPORT 31Oct19 JER DIMA 5.4 as of 2019-04-19.mdb"

path8 = r'C:\Users\kbonefont\Desktop\other dima\REPORT 3Aug16 - 27Oct17 Mandan DIMA 5.3 as of 2018-02-14.mdb'
path9 = r'C:\Users\kbonefont\Desktop\dimas\DIMA 5.2 as of 2017-07-18.mdb'
path10 = r'C:\Users\kbonefont\Desktop\dimas\LCDO_OMDPNM_2018_Final.mdb'

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


main_translate('tblGapHeader', path10)
    # full = translate_table[tablename]
    # if tableswitch[tablename]=="no pk":
    #
    #         return full
    # else:
    #     iso = arc.isolateFields(full,tableswitch[tablename],"PrimaryKey").copy()
    #     iso.drop_duplicates([tableswitch[tablename]],inplace=True)
    #
    #     target_table = arcno.MakeTableView(tablename, dimapath)
    #     retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
    #     return retdf


main_translate('tblSpecies', path10)
a = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric','tblSites']
b = ['tblSoilStabDetail', 'tblSoilStabHeader']
c = ['tblSoilPits','tblSoilPitHorizons']
d = ['tblPlantProdDetail', 'tblPlantProdHeader']
types={
'a':(None, dimapath, tablename),
'b':('soilstab',dimapath, None),
'c':('soilpits',dimapath, None),
'd':('plantprod',dimapath, None),
'e':(dimapath)
}
therest =


translate_table = {
    'tblPlots':no_pk(None, dimapath, tablename),
    'tblLines':no_pk(None, dimapath, tablename),
    'tblLPIDetail':lpi_pk(dimapath),
    'tblLPIHeader':lpi_pk(dimapath),
    'tblGapDetail':gap_pk(dimapath),
    'tblGapHeader':gap_pk(dimapath),
    # 'tblQualHeader':no_pk(dimapath),
    # 'tblQualDetail':no_pk(dimapath),
    'tblSoilStabHeader':no_pk('soilstab',dimapath,None),
    'tblSoilStabDetail':no_pk('soilstab',dimapath,None),
    'tblSoilPitHorizons':no_pk('soilpits',dimapath,None),
    'tblSoilPits':no_pk('soilpits',dimapath,None),
    'tblSpecRichHeader':sperich_pk(dimapath),
    'tblSpecRichDetail':sperich_pk(dimapath),
    'tblPlantProdHeader':no_pk('plantprod',dimapath,None),
    'tblPlantProdDetail':no_pk('plantprod',dimapath,None),
    # 'tblPlotNotes',
    'tblPlantDenHeader':plantden_pk(dimapath),
    'tblPlantDenDetail':plantden_pk(dimapath),
    'tblSpecies':no_pk(None, dimapath, tablename)
    'tblSpeciesGeneric':no_pk(None, dimapath, tablename),
    'tblSites':no_pk(None, dimapath, tablename),
    'tblBSNE_Box':bsne_pk(dimapath),
    'tblBSNE_BoxCollection':bsne_pk(dimapath),
    'tblBSNE_Stack':bsne_pk(dimapath),
    'tblBSNE_TrapCollection':bsne_pk(dimapath)
}

translate_table = {
    'tblPlots':no_pk,
    'tblLines':no_pk,
    'tblLPIDetail':lpi_pk ,
    'tblLPIHeader':lpi_pk ,
    'tblGapDetail':gap_pk ,
    'tblGapHeader':gap_pk ,
    # 'tblQualHeader':no_pk ,
    # 'tblQualDetail':no_pk ,
    'tblSoilStabHeader':no_pk ,
    'tblSoilStabDetail':no_pk ,
    'tblSoilPitHorizons':no_pk ,
    'tblSoilPits':no_pk ,
    'tblSpecRichHeader':sperich_pk ,
    'tblSpecRichDetail':sperich_pk ,
    'tblPlantProdHeader':no_pk,
    'tblPlantProdDetail':no_pk,
    # 'tblPlotNotes',
    'tblPlantDenHeader':plantden_pk ,
    'tblPlantDenDetail':plantden_pk ,
    'tblSpecies':no_pk
    'tblSpeciesGeneric':no_pk,
    'tblSites':no_pk,
    'tblBSNE_Box':bsne_pk ,
    'tblBSNE_BoxCollection':bsne_pk ,
    'tblBSNE_Stack':bsne_pk ,
    'tblBSNE_TrapCollection':bsne_pk
}



switcher = {
    'tblLines':no_pk
}
switcher['tblLines'](None,path10,'tblLines')
switcher.get('tblLines')(None,path10,'tblLines')

tuple = (None, path10, 'tblLines')

def switch(tablename, dimapath):




unpack = {
'a':(None, path10, 'tblLines')
}
unpack['a']
switcher['tblLines'](*unpack['a'])















prodd = arcno.MakeTableView('tblPlantProdDetail', path10)
prodh = arcno.MakeTableView('tblPlantProdHeader', path10)
gaph = arcno.MakeTableView('tblGapHeader', path10)
gapd = arcno.MakeTableView('tblGapDetail', path10)

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

def translate_table(tablefam=None, dimapath=None,tablename=None):

    return d[tablename]

translate_table(path10,'tblGapDetail')
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
arc = arcno(path10,False)

arcno.MakeTableView('tblPlantProdHeader', path10)
arc.tablelist
arc.actual_list
a=arcno.MakeTableView('tblGapDetail', path10)
"""
different formdate for pk
gapheader = 45
sperichheader = 60
soilstabheader = 43
which to choose for plots, lines, soilpits
lpi -plots and lines,
soil pits,
lpi for ericka

pk assume

compare nri pasture

"""

pd.merge(a,b,how="inner",on="SoilKey")

'LCDO' in path10
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
    'tblPlots':no_pk(None, dimapath, tablename),
    'tblLines':no_pk(None, dimapath, tablename),
    'tblLPIDetail':lpi_pk(dimapath),
    'tblLPIHeader':lpi_pk(dimapath),
    'tblGapDetail':gap_pk(dimapath),
    'tblGapHeader':gap_pk(dimapath),
    # 'tblQualHeader':no_pk(dimapath),
    # 'tblQualDetail':no_pk(dimapath),
    'tblSoilStabHeader':no_pk('soilstab',dimapath,None),
    'tblSoilStabDetail':no_pk('soilstab',dimapath,None),
    'tblSoilPitHorizons':no_pk('soilpits',dimapath,None),
    'tblSoilPits':no_pk('soilpits',dimapath,None),
    'tblSpecRichHeader':sperich_pk(dimapath),
    'tblSpecRichDetail':sperich_pk(dimapath),
    'tblPlantProdHeader':no_pk('plantprod',dimapath,None),
    'tblPlantProdDetail':no_pk('plantprod',dimapath,None),
    # 'tblPlotNotes',
    'tblPlantDenHeader':plantden_pk(dimapath),
    'tblPlantDenDetail':plantden_pk(dimapath),
    'tblSpecies':no_pk(None, dimapath, tablename),
    'tblSpeciesGeneric':no_pk(None, dimapath, tablename),
    'tblSites':no_pk(None, dimapath, tablename),
    'tblBSNE_Box':bsne_pk(dimapath),
    'tblBSNE_BoxCollection':bsne_pk(dimapath),
    'tblBSNE_Stack':bsne_pk(dimapath),
    'tblBSNE_TrapCollection':bsne_pk(dimapath)
    }

    full = translate_table[tablename]
    if tableswitch[tablename]=="no pk":
        if 'tblSpecies' in tablename and 'LCDO' in dimapath:
            full.SortSeq = full.SortSeq.astype("Int64")
            return full
        else:
            return full
    else:
        iso = arc.isolateFields(full,tableswitch[tablename],"PrimaryKey").copy()
        iso.drop_duplicates([tableswitch[tablename]],inplace=True)

        target_table = arcno.MakeTableView(tablename, dimapath)
        retdf = pd.merge(target_table, iso, how="inner", on=tableswitch[tablename])
        return retdf
p1 = arcno.MakeTableView('tblSpecies', path1)
p10 = arcno.MakeTableView('tblSpecies', path10)

p1.SortSeq.unique()
pd.NA
p10.SortSeq.astype("Int64")

p10.SortSeq = p10.SortSeq.astype('Int64')
p10[p10.SortSeq.astype("Int64")==99]
no_pk(None, path1, 'tblSpecies')
main_translate('tblSpecies',path10)






















            # modcheck+=1
