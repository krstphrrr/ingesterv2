from src.utils.arcnah import arcno
import pandas as pd
import numpy as np
from src.projects.dima.tables.lpipk import lpi_pk
from src.projects.dima.tables.bsnepk import bsne_pk
import platform

def no_pk(tablefam:str=None,dimapath:str=None,tablename:str= None):
    """

    """
    arc = arcno()
    fam = {
        'plantprod':['tblPlantProdDetail','tblPlantProdHeader'],
        'soilstab':['tblSoilStabDetail','tblSoilStabHeader'],
        'soilpit':['tblSoilPits', 'tblSoilPitHorizons'],
        'plantden':['tblPlantDenDetail','tblPlantDenHeader'],
        }
    try:
        if tablefam is not None and ('plantprod' in tablefam):

            header = arcno.MakeTableView(fam['plantprod'][1],dimapath)
            detail = arcno.MakeTableView(fam['plantprod'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det.FormDate = pd.to_datetime(head_det.FormDate) if platform.system()=='Linux' else head_det.FormDate

            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            return head_det


        elif tablefam is not None and ('soilstab' in tablefam):
            header = arcno.MakeTableView(fam['soilstab'][1],dimapath)
            detail = arcno.MakeTableView(fam['soilstab'][0],dimapath)
            head_det = pd.merge(header,detail,how="inner", on="RecKey")
            head_det.FormDate = pd.to_datetime(head_det.FormDate) if platform.system()=='Linux' else head_det.FormDate
            head_det = arc.CalculateField(head_det,"PrimaryKey","PlotKey","FormDate")
            if tablename=="tblSoilStabHeader":

                iso = arc.isolateFields(head_det,'PlotKey','PrimaryKey').copy()
                merge = pd.merge(header, iso, how="inner", on="PlotKey").drop_duplicates()
                return merge
            elif tablename=="tblSoilStabDetail":

                iso = arc.isolateFields(head_det,'RecKey','PrimaryKey').copy()
                merge = pd.merge(detail, iso, how="inner", on="RecKey").drop_duplicates(subset=["BoxNum"])
                return merge


        elif tablefam is not None and ('soilpit' in tablefam):
            # print("soilpit")
            pits = arcno.MakeTableView(fam['soilpit'][0], dimapath)
            horizons = arcno.MakeTableView(fam['soilpit'][1], dimapath)
            merge = pd.merge(pits, horizons, how="inner", on="SoilKey")

            allpks = lpi_pk(dimapath)
            pks = allpks.loc[:,["PrimaryKey", "EstablishDate", "FormDate", "DateModified", "PlotKey"]].copy()
            iso = arc.isolateFields(pks,'PlotKey','PrimaryKey').copy()
            premerge = pd.merge(pits_horizons,iso,how="inner", on="PlotKey").drop_duplicates().copy()
            if tablename=="tblSoilPits":
                iso = arc.isolateFields(premerge, 'PlotKey', 'PrimaryKey').drop_duplicates().copy()
                merge = pd.merge(pits,iso,how="inner",on="PlotKey")
                return merge
            elif tablename=="tblSoilPitHorizons":
                iso = arc.isolateFields(premerge,'HorizonKey', 'PrimaryKey').drop_duplicates().copy()
                merge = pd.merge(horizons,iso,how="inner",on="HorizonKey")
                return merge

        elif tablefam is not None and ('plantden' in tablefam):
            dendet = arcno.MakeTableView(fam['plantden'][0], dimapath)
            denhead = arcno.MakeTableView(fam['plantden'][1], dimapath)
            plantden = pd.merge(denhead,dendet, how="inner", on="RecKey")
            allpks = lpi_pk(dimapath)
            pks = allpks.loc[:,["PrimaryKey", "EstablishDate", "FormDate", "RecKey","LineKey"]].copy()
            iso = arc.isolateFields(pks,'LineKey','PrimaryKey').copy()
            premerge = pd.merge(plantden,iso,how="inner", on="LineKey").drop_duplicates().copy()
            if tablename=="tblPlantDenHeader":
                iso =  arc.isolateFields(premerge,'LineKey','PrimaryKey').copy()
                merge = pd.merge(denhead,iso, how="inner", on="LineKey")
                return merge
            elif tablename=="tblPlantDenDetail":
                iso =  arc.isolateFields(premerge,'RecKey','PrimaryKey').copy()
                merge = pd.merge(dendet,iso, how="inner", on="RecKey")
                return merge

        else:
            no_pk_df = arcno.MakeTableView(tablename, dimapath)
            # print('netdima in path')
            if ('Network_DIMAs' in dimapath) and (tablefam==None):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print("lines,plots; networkdima in the path")
                    fulldf = bsne_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("network, but not line or plot, no pk")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df

            elif ('Network_DIMAs' in dimapath) and ('fake' in tablefam):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()

                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("network, but not line or plot, no pk --fakebranch")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df

            else:
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print('not network, no tablefam')
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["LineKey","PrimaryKey"]) if "tblLines" in tablename else pd.merge(no_pk_df,iso,how="inner",on=["PlotKey"]).drop_duplicates(["PrimaryKey"])
                    return no_pk_df
                else:
                    print("not network, not line or plot, no pk")
                    if 'Sites' in tablename:
                        no_pk_df = no_pk_df[(no_pk_df.SiteKey!='888888888') & (no_pk_df.SiteKey!='999999999')]
                        return no_pk_df
                    else:
                        return no_pk_df
            # return no_pk_df
    except Exception as e:
        print(e)

def date_column_chooser(df,iso):

    if "FormDate" in df.columns:
        df.FormDate = pd.to_datetime(df.FormDate)
    if "FormDate2" in df.columns:
        df.FormDate2 = pd.to_datetime(df.FormDate2)
    if "FormDate" in iso.columns:
        iso.FormDate = pd.to_datetime(iso.FormDate)
    if "EstablishDate" in iso.columns:
        iso.EstablishDate = pd.to_datetime(iso.EstablishDate)

    # df.FormDate = pd.to_datetime(df.FormDate) if platform.system()=='Linux' else df.FormDate
    # iso.FormDate = pd.to_datetime(iso.FormDate) if platform.system()=='Linux' else iso.FormDate
    # df.FormDate2 = pd.to_datetime(df.FormDate2) if platform.system()=='Linux' else df.FormDate2
    # iso.EstablishDate = pd.to_datetime(iso.EstablishDate) if platform.system()=='Linux' else iso.EstablishDate

    df_establish = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="EstablishDate").drop_duplicates('HorizonKey')
    df_formdate = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="FormDate").drop_duplicates('HorizonKey')
    if np.nan not in [i for i in df_formdate.PrimaryKey]:
        return df_formdate
    elif np.nan not in [i for i in df_establish.PrimaryKey]:
        return df_establish
    else:
        iso["DateModified2"] = pd.to_datetime(iso.DateModified.apply(lambda x: pd.Timestamp(x).date()))
        df_datemod = pd.merge(df, iso, how="left", left_on="FormDate2", right_on="DateModified2").drop_duplicates('HorizonKey')
        return df_datemod
