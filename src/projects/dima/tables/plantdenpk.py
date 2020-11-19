from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tabletools import fix_fields
import platform

def plantden_pk(dimapath):
    """
    DEPRECATED
    returns a dataframe with tblplots, tbllines, tblplantdenheader and tblplantdenDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
    arc = arcno()
    denhead = arcno.MakeTableView('tblPlantDenHeader', dimapath)
    dendet = arcno.MakeTableView('tblPlantDenDetail', dimapath)
    # dendet = arcno.MakeTableView(fam['plantden'][0], dimapath)
    # denhead = arcno.MakeTableView(fam['plantden'][1], dimapath)
    plantden = pd.merge(denhead,dendet, how="inner", on="RecKey")

    if 'tblLPIDetail' in ins.actual_list:
        allpks = lpi_pk(dimapath)
    elif 'tblGapDetail' in ins.actual_list:
        allpks = gap_pk(dimapath)
    else:
        print("a difficult one! no source of easy source of PK's in this dima!")
        # where to pull them from will depend on which table needs em, and
        # what that table has in terms of fields (plotkey, reckey, linekey etc.)
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
