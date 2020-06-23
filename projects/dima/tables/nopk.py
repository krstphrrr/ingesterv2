from utils.arcnah import arcno
import pandas as pd
from projects.dima.tables.lpipk import lpi_pk

def no_pk(debug=False,tablefam:str=None,dimapath:str=None,tablename:str= None):
    """
    creates and appends PrimaryKey field for tables:
    tblPlantProdDetail, tblPlantProdHeader, tblPlots, tblLines
    tblSoilStabDetail, tblSoilStabHeader.
    if table = tblSpecies, tblSpeciesGeneric, tblSites, no primarykey is appended

    - soil pits still need source for plotkey/formdate primarykey;
    unclear if lpi coincides in all dimas with soilpits.

    - tblplots and tblLines get primarykey from LPI if not from networkdima,
    else it gets its primarykey from plotkey/collectdate like the rest of bsne
    tables.

    """
    arc = arcno()
    fam = {
        'plantprod':['tblPlantProdDetail','tblPlantProdHeader'],
        'soilstab':['tblSoilStabDetail','tblSoilStabHeader'],
        'soilpit':['tblSoilPits', 'tblSoilPitHorizons']
    }
    try:
        if tablefam is not None and ('plantprod' in tablefam):

            header = arcno.MakeTableView(fam['plantprod'][1],dimapath)
            detail = arcno.MakeTableView(fam['plantprod'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            return head_det


        elif tablefam is not None and ('soilstab' in tablefam):
            header = arcno.MakeTableView(fam['soilstab'][1],dimapath)
            detail = arcno.MakeTableView(fam['soilstab'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            return head_det

        elif tablefam is not None and ('soilpit' in tablefam):
            pits = arcno.MakeTableView(fam['soilpit'][0], dimapath)
            horizons = arcno.MakeTableView(fam['soilpit'][1], dimapath)
            merge = pd.merge(pits, horizons, how="inner", on="SoilKey")
            return merge

        else:
            no_pk_df = arcno.MakeTableView(tablename, dimapath)
            if ('Network_DIMAs' in dimapath) and (tablefam==None):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = bsne_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    return no_pk_df

            elif ('Network_DIMAs' in dimapath) and ('fake' in tablefam):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    return no_pk_df
            else:
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print('not network, no tablefam')
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    return no_pk_df
                else:
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    return no_pk_df
            # return no_pk_df
    except Exception as e:
        print(e)
