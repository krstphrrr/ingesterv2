from utils.arcnah import arcno
import pandas as pd


def sperich_pk(dimapath):
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

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk
