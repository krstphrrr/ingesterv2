from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tabletools import fix_fields
import platform

def bsne_pk(dimapath):
    """
    returns a dataframe with tblplots, tblBSNE_Box, tblBSNE_Stack and
    tblBSNE_BoxCollection and tblBSNE_TrapCollection joined. if tblBSNE_TrapCollection
    does not exist in networkdima, skip it and join: box, stack and boxcollection.
    if it exists, join trapcollection and stack.

    PrimaryKey field is made using formdate and plotkey

    """
    arc = arcno(dimapath)
    if "tblBSNE_TrapCollection" in arc.actual_list:
        ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    else:
        ddt = pd.DataFrame({'A' : []})

    if ddt.shape[0]>0:
        ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)

        stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)

        # df = arc.AddJoin(stack, ddt, "StackID", "StackID")
        df = pd.merge(stack,ddt, how="inner", on="StackID")
        # df.collectDate = pd.to_datetime(df.collectDate) if platform.system()=='Linux' else df.collectDate
        if "collectDate" in df.columns:
            df.collectDate = pd.to_datetime(df.collectDate)

        df2 = arc.CalculateField(df,"PrimaryKey","PlotKey","collectDate")

        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")
        return df2tmp3

    else:

        box = arcno.MakeTableView("tblBSNE_Box",dimapath)
        stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
        boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)


        plotted_boxes = pd.merge(box,stack, how="inner", on="StackID")

        collected_boxes = pd.merge(plotted_boxes,boxcol, how="inner", on="BoxID")
        df2 = arc.CalculateField(collected_boxes,"PrimaryKey","PlotKey","collectDate")
        df2.collectDate = pd.to_datetime(df2.collectDate) if platform.system()=='Linux' else df2.collectDate

        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")
        return df2tmp3
