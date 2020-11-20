from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tabletools import fix_fields
import platform

def sperich_pk(dimapath, tablename):
    """
    returns a dataframe with tblplots, tbllines, tblsperichheader and tblsperichDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
    arc = arcno()
    spe_header = arcno.MakeTableView('tblSpecRichHeader', dimapath)
    spe_detail = arcno.MakeTableView('tblSpecRichDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots, lines, how="inner", on="PlotKey")
    spehead_detail = pd.merge(spe_header, spe_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line, spehead_detail, how="inner", on="LineKey")
    plot_line_det.FormDate = pd.to_datetime(plot_line_det.FormDate) if platform.system()=='Linux' else plot_line_det.FormDate
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")
    if tablename=="tblSpecRichHeader":
        iso = arc.isolateFields(plot_pk, "LineKey","PrimaryKey")
        merge = pd.merge(spe_header,iso, how="inner", on="LineKey")
        return merge
    elif tablename=="tblSpecRichDetail":
        iso = arc.isolateFields(plot_pk, "RecKey","PrimaryKey")
        merge = pd.merge(spe_detail,iso, how="inner", on="RecKey")
        return merge
