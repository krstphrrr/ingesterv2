from utils.arcnah import arcno
import pandas as pd

def no_pk(tablefam=None,dimapath=None,tablename = None):
    arc = arcno()
    fam = {
        'plantprod':['tblPlantProdDetail','tblPlantProdHeader'],
        'soilstab':['tblSoilStabDetail','tblSoilStabHeader']
    }
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
    else:
        no_pk_df = arcno.MakeTableView(tablename, dimapath)
        return no_pk_df

def lpi_pk(dimapath):
    # tables
    # arc = arcno()
    lpi_header = arcno.MakeTableView('tblLPIHeader', dimapath)
    lpi_detail = arcno.MakeTableView('tblLPIDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots, lines, how="inner", on="PlotKey")
    lpihead_detail = pd.merge(lpi_header, lpi_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line, lpihead_detail, how="inner", on="LineKey")
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
    plot_line = pd.merge(plots,lines,how="inner", on="PlotKey")
    gaphead_detail = pd.merge(gap_header,gap_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line,gaphead_detail,how="inner", on="LineKey")
    # fixing dup fields
    tmp1 = fix_fields(plot_line_det, 'DateModified')
    tmp2 = fix_fields(tmp1, 'ElevationType')

    plot_pk = arc.CalculateField(tmp2, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def sperich_pk(dimapath):
    # tables
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

def plantden_pk(dimapath):
    # tables
    arc = arcno()
    pla_header = arcno.MakeTableView('tblPlantDenHeader', dimapath)
    pla_detail = arcno.MakeTableView('tblPlantDenDetail', dimapath)
    lines = arcno.MakeTableView('tblLines', dimapath)
    plots = arcno.MakeTableView('tblPlots', dimapath)
    # joins
    plot_line = pd.merge(plots,lines, how="inner" ,on='PlotKey')
    plahead_detail = pd.merge(pla_header,pla_detail, how="inner" ,on='RecKey')

    plot_line_det = pd.merge(plot_line, plahead_detail,how="inner", on='LineKey')

    plot_pk = arc.CalculateField(plot_line_det, "PrimaryKey", "PlotKey", "FormDate")

    return plot_pk

def bsne_pk(dimapath):
    ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    arc = arcno()
    if ddt.shape[0]>0:
        ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)

        stack = arc.MakeTableView("tblBSNE_Stack", dimapath)

        # df = arc.AddJoin(stack, ddt, "StackID", "StackID")
        df = pd.merge(stack,ddt, how="inner", on="StackID")
        df2 = arc.CalculateField(df,"PrimaryKey","PlotKey","collectDate")
        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")
        return df2
    else:

        box = arcno.MakeTableView("tblBSNE_Box",dimapath)
        stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
        boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)
        # differences 1

        # dfx = pd.merge(stack, box[cols_dif1], left_index=True, right_index=True, how="outer")
        df = pd.merge(box,stack, how="inner", on="StackID")
        df2 = pd.merge(df,boxcol, how="inner", on="BoxID")
        # fix
        df2tmp = fix_fields(df2,"Notes")
        df2tmp2 = fix_fields(df2tmp,"DateModified")
        df2tmp3 = fix_fields(df2tmp2,"DateEstablished")
        df2 = arc.CalculateField(df2tmp3,"PrimaryKey","PlotKey","collectDate")
        return df2


def fix_fields(df : pd.DataFrame, keyword: str):
    df = df.copy()
    done=False
    while done!=True:
        if len([i for i in df.columns if f'{keyword}' in i])>2:
            df.drop([f'{keyword}_y',f'{keyword}_x'], axis=1, inplace=True)
            # print('aqui 1')
            done=True
            return df
        else:
            if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
                if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                    # if the two notes are the same, keep one of them.
                    df.drop([f'{keyword}_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    # print('aqui 2')
                    done=True
                    return df


                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the x is none, keep the y
                    df.drop([f'{keyword}_x'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)
                    # print('aqui 3')
                    done=True
                    return df

                elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                    # if the two notes are different AND the y is none, keep the x
                    df.drop(['Notes_y'], axis=1, inplace=True)
                    df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)
                    # print('aqui 4')
                    done=True
                    return df

            else:
                return df
