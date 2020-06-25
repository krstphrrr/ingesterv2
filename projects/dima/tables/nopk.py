from utils.arcnah import arcno
import pandas as pd
from projects.dima.tables.lpipk import lpi_pk

def no_pk(tablefam:str=None,dimapath:str=None,tablename:str= None):
    """

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
            print("soilpit")
            pits = arcno.MakeTableView(fam['soilpit'][0], dimapath)
            horizons = arcno.MakeTableView(fam['soilpit'][1], dimapath)
            merge = pd.merge(pits, horizons, how="inner", on="SoilKey")

            allpks = lpi_pk(dimapath)
            iso = allpks.loc[:,["PrimaryKey", "FormDate"]].copy()
            merge['FormDate2'] = pd.to_datetime(merge.DateRecorded.apply(lambda x: pd.Timestamp(x).date()))

            mergepk = pd.merge(merge, iso, how="left", left_on="FormDate2", right_on="FormDate").drop_duplicates('HorizonKey')
            mergepk.drop(['FormDate','FormDate2'], axis=1, inplace=True)

            return mergepk

        else:
            no_pk_df = arcno.MakeTableView(tablename, dimapath)
            if ('Network_DIMAs' in dimapath) and (tablefam==None):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print("lines,plots; networkdima in the path")
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    # iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df

            elif ('Network_DIMAs' in dimapath) and ('fake' in tablefam):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    # iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
            else:
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print('not network, no tablefam')
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    # iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("not network, not line or plot, no pk")
                    # fulldf = lpi_pk(dimapath)
                    # iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    # iso.drop_duplicates(['PlotKey'],inplace=True)
                    # no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    return no_pk_df
            # return no_pk_df
    except Exception as e:
        print(e)
