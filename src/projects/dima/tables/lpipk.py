from src.utils.arcnah import arcno
import pandas as pd
from src.projects.dima.tabletools import fix_fields
import platform

def lpi_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tbllpiheader and tblLPIDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """

    lpi_header = arcno.MakeTableView('tblLPIHeader', dimapath)
    lpi_detail = arcno.MakeTableView('tblLPIDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)

    # joins

    plot_line = pd.merge(plots, lines, how="inner", on="PlotKey")
    plot_line.LineKey
    lpihead_detail = pd.merge(lpi_header, lpi_detail, how="inner", on="RecKey")
    len(lpihead_detail.PointLoc.unique())

    plot_line_det = pd.merge(plot_line, lpihead_detail, how="inner", on="LineKey")
    plot_line_det.loc[:,['FormDate',"RecKey", "LineKey"]]
    arc = arcno()
    #
    # tmp1 = fix_fields(plot_line_det, 'DateModified').copy()
    # tmp2 = fix_fields(tmp1,'ElevationType').copy()
    plot_line_det.FormDate = pd.to_datetime(plot_line_det.FormDate) if platform.system()=='Linux' else plot_line_det.FormDate
    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")
    # plot_pk.drop_duplicates(["PrimaryKey", "PlotKey", "FormDate"])

    return plot_pk
