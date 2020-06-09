from utils.arcnah import arcno
from os.path import normpath, split, splitext, join
from utils.tools import db
from sqlalchemy import create_engine
from utils.tools import  config
from datetime import datetime
from psycopg2 import sql
import pandas as pd

"""
blm - work;
jornada symposium : monitoring data, connecting
working grooup eltar: rangeland soil eroison

collaborative projects: to leverage the datacommons// boise tucson reno//
 - data manager now haha


"""
for i in box.StackID:
    if i in stack.StackID:
        print(i)
import pandas as pd
pd.merge(box,stack, how="inner", on="StackID")


dimapath = r'C:\Users\kbonefont\Desktop\Network_DIMAs\8May2017 DIMA 5.5a as of 2020-03-10.mdb'
bsne_pk(dimapath)
lpi_pk(path)
gap_pk(path)
sperich_pk(path)
arc = arcno(path)
arc.maintablelist
arcno.MakeTableView('tblLines', path).shape
for i in arc.maintablelist:
    if arc.MakeTableView(i, path).shape[0]>1:
        print(i)

sp=arcno.MakeTableView("tblSpecies",dimapath)
spg=arcno.MakeTableView("tblSpeciesGeneric", dimapath)
arcno.MakeTableView("tblSites", dimapath)
arcno.MakeTableView("tblBSNE_Box", dimapath)
pd.merge(sp,spg, how="inner", on="SpeciesCode")

spg.CommonName.unique()
spg.FinalCode.unique()


"""
from path to ingest:

1. path and table name into  'pg_send'
2. if 'pg_send' finds 'BSNE_Box', 'BSNE_BoxCollection' and 'BSNE_TrapCollection' ==>
 - bsne_pk : adds primary key to lowerlevel, joins and propagates pk upstream

   if NOT ==>
 - pk_add : regular table name and path
"""

gap
arc.isolateFields()

def pg_send(tablename,dimapath):
    plot = None

    plotkeylist = ['tblPlots','tblLines','tblQualHeader','tblSoilStabHeader',
    'tblSoilPits','tblPlantProdHeader','tblPlotNotes', 'tblSoilPitHorizons']

    linekeylist = ['tblGapHeader','tblLPIHeader','tblSpecRichHeader',
    'tblPlantDenHeader']

    reckeylist = ['tblGapDetail','tblLPIDetail','tblQualDetail','tblSoilStabDetail',
    'tblSpecRichDetail','tblPlantProdDetail','tblPlantDenDetail']

    nonline = {'tblQualDetail':'tblQualHeader',
    'tblSoilStabDetail':'tblSoilStabHeader','tblPlantProdDetail':'tblPlantProdHeader'}

    if tablename in plotkeylist:



def lpi_pk(dimapath):
    # tables
    # arc = arcno()
    lpi_header = arcno.MakeTableView('tblLPIHeader', dimapath)
    lpi_detail = arcno.MakeTableView('tblLPIDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arcno.AddJoin(plots,lines, 'PlotKey','PlotKey')
    lpihead_detail = arcno.AddJoin(lpi_header, lpi_detail, 'RecKey')
    plot_line_det = arcno.AddJoin(plot_line, lpihead_detail, 'LineKey', 'LineKey')
    arc = arcno()
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def gap_pk(dimapath):
    # tables
    arc = arcno()
    gap_header = arcno.MakeTableView('tblGapHeader', dimapath)
    gap_detail = arcno.MakeTableView('tblGapDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    gaphead_detail = arc.AddJoin(gap_header, gap_detail, 'RecKey')
    gaphead_detail = pd.merge(gap_header,gap_detail, how="inner", on="RecKey")

    plot_line_det = arc.AddJoin(plot_line, gaphead_detail, 'LineKey', 'LineKey')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def sperich_pk(dimapath):
    # tables
    arc = arcno()
    spe_header = arcno.MakeTableView('tblSpecRichHeader', dimapath)
    spe_detail = arcno.MakeTableView('tblSpecRichDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arcno.AddJoin(plots,lines, 'PlotKey','PlotKey')
    spehead_detail = arcno.AddJoin(spe_header, spe_detail, 'RecKey')
    plot_line_det = arcno.AddJoin(plot_line, spehead_detail, 'LineKey', 'LineKey')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def plantden_pk(dimapath):
    # tables
    arc = arcno()
    pla_header = arc.MakeTableView('tblPlantDenHeader', dimapath)
    pla_detail = arc.MakeTableView('tblPlantDenDetail', dimapath)
    lines = arc.MakeTableView('tblLines', dimapath)
    plots = arc.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = arc.AddJoin(plots,lines, 'PlotKey','PlotKey')
    plahead_detail = arc.AddJoin(pla_header, pla_detail, 'RecKey')
    plot_line_det = arc.AddJoin(plot_line, plahead_detail, 'LineKey', 'LineKey')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk
tst = bsne_pk(dimapath)
tst.Notes
for i in tst.columns:
    if 'Notes' in i:
        print(i)
 tst.Notes_x.equals(tst.Notes_y)
tst.columns.difference()
tst
modcheck=0
finish = False
type(tst.Notes_x)
def test(column:pd.Series)
len([i for i in tst.columns if 'Notes' in i])
def fix_fields(df : pd.DataFrame, keyword: str):
    df = df.copy()
    done=False
    while done!=True:
        if len([i for i in df.columns if f'{keyword}' in i])>2:
            df.drop([f'{keyword}_y',f'{keyword}_x'], axis=1, inplace=True)
            print('aqui 1')
            done=True
            return df
        else:
            if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
                if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                    # if the two notes are the same, keep one of them.
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    print('aqui 2')
                    done=True
                    return df


                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the x is none, keep the y
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)
                    print('aqui 3')
                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the y is none, keep the x
                    df.drop(['Notes_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    print('aqui 4')
                    done=True
                    return df

            else:
                return df
            # modcheck+=1
notes = fix_fields(tst, "Notes")

notes.DateModified_x
notes.DateModified_y
modified = fix_fields(notes,"DateModified")
modified
est = fix_fields(modified,"DateEstablished")
est.columns
len(tst.Notes_x)
len(tst.Notes_x==None)
len([i for i in tst.Notes_x if i==None])
len([i for i in tst.Notes_y if i==None])
notes.Notes
while finish!=True:
    """ this while loop fixes column names that duplicate when dataframes merge """
    if ('DateEstablished_x' in tst.columns) or ('DateEstablished_y' in tst.columns) and modcheck==0:
        if tst.DateEstablished_x.equals(tst.DateEstablished_y):
            # if both fields are the same, drop one keep the other
            tst.drop(['DateEstablished_y'], axis=1, inplace=True)
            tst.rename(columns={"DateEstablished_x":"DateEstablished"}, inplace=True)
            modcheck+=1

    else:
        pass
        modcheck+=1

    if ('Notes_x' in tst.columns) or ('Notes_y' in tst.columns) and modcheck==1:
        if tst.Notes_x.equals(tst.Notes_y):
            # if the two notes are the same, keep one of them.
            tst.drop(['Notes_y'], axis=1, inplace=True)
            tst.rename(columns={"Notes_x":"Notes"}, inplace=True)
            modcheck+=1
        elif (tst.Notes_x.equals(tst.Notes_y)==False) and all(tst.Notes_x.unique()==None):
            # if the two notes are different AND the x is none, keep the y
            tst.drop(['Notes_x'], axis=1, inplace=True)
            tst.rename(columns={"Notes_y":"Notes"}, inplace=True)
            modcheck+=1
        elif (tst.Notes_x.equals(tst.Notes_y)==False) and all(tst.Notes_y.unique()==None):
            # if the two notes are different AND the y is none, keep the x
            tst.drop(['Notes_y'], axis=1, inplace=True)
            tst.rename(columns={"Notes_x":"Notes"}, inplace=True)
            modcheck+=1
    else:
        pass
        modcheck+=1
    if 'DateModified_x' in tst.columns and modcheck==2:
        finish=True
        print('ok')


def bsne_pk(dimapath):
    ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    arc = arcno()
    if ddt.shape[0]>0:
        ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)

        stack = arc.MakeTableView("tblBSNE_Stack", dimapath)

        # df = arc.AddJoin(stack, ddt, "StackID", "StackID")
        df = pd.merge(stack,ddt, how="inner", on="StackID")
        df2 = arc.CalculateField(df,"PrimaryKey","PlotKey","collectDate")
        return df2
    else:

        box = arcno.MakeTableView("tblBSNE_Box",dimapath)
        stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
        boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)
        # differences 1
        cols_dif1 = box.columns.difference(stack.columns)
        dfx = pd.merge(stack, box[cols_dif1], left_index=True, right_index=True, how="outer")
        df = pd.merge(box,stack, how="inner", on="StackID")
        df2 = pd.merge(df,boxcol, how="inner", on="BoxID")
        df2 = arc.CalculateField(df2,"PrimaryKey","PlotKey","collectDate")
        return df2





        return df2


for i in cols_dif1:
    if i not in stack.columns:
        print(i)
finish=False
count=0
while finish!=True:
    if count==0:
        print('a')
        count+=1
    if count==1:
        print('b')
        count+=1
    if count==2:
        print('c')
        count+=1
        finish=True
else:
    print('acabe')
