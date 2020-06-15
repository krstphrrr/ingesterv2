from utils.arcnah import arcno
import pandas as pd


def no_pk(tablefam=None,dimapath=None,tablename = None):
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

    -

    todo: need to include tblQualDetail,tblQualHeader, tblPlotNotes


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
                    # return no_pk_df

            elif ('Network_DIMAs' in dimapath) and ('fake' in tablefam):
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    # return no_pk_df
            else:
                if ('tblPlots' in tablename) or ('tblLines' in tablename):
                    print('not network, no tablefam')
                    fulldf = lpi_pk(dimapath)
                    iso = arc.isolateFields(fulldf,'PlotKey','PrimaryKey').copy()
                    iso.drop_duplicates(['PlotKey'],inplace=True)
                    no_pk_df = pd.merge(no_pk_df,iso,how="inner",on="PlotKey")
                    # return no_pk_df
            return no_pk_df
    except Exception as e:
        print(e)

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
    lpihead_detail = pd.merge(lpi_header, lpi_detail, how="inner", on="RecKey")
    plot_line_det = pd.merge(plot_line, lpihead_detail, how="inner", on="LineKey")
    arc = arcno()

    tmp1 = fix_fields(plot_line_det, 'DateModified')
    tmp2 = fix_fields(tmp1,'ElevationType')
    plot_pk = arc.CalculateField(tmp2, "PrimaryKey", "PlotKey", "FormDate")
    return plot_pk

def gap_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tblgapheader and tblgapDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
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

def plantden_pk(dimapath):
    """
    returns a dataframe with tblplots, tbllines, tblplantdenheader and tblplantdenDetail
    joined. PrimaryKey field is made using formdate and plotkey

    """
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
    """
    returns a dataframe with tblplots, tblBSNE_Box, tblBSNE_Stack and
    tblBSNE_BoxCollection and tblBSNE_TrapCollection joined. if tblBSNE_TrapCollection
    does not exist in networkdima, skip it and join: box, stack and boxcollection.
    if it exists, join trapcollection and stack.

    PrimaryKey field is made using formdate and plotkey

    """
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
        if (f'{keyword}_x' in df.columns) or (f'{keyword}_y' in df.columns):
            if df[f'{keyword}_x'].equals(df[f'{keyword}_y']):
                # if the two notes are the same, keep one of them.
                df.drop([f'{keyword}_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())>len(df[f'{keyword}_y'].unique())):
                # if the two notes are different AND the x is none, keep the y
                df.drop([f'{keyword}_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and ((None not in df[f'{keyword}_x']) or (None not in df[f'{keyword}_x'])) and (len(df[f'{keyword}_x'].unique())<len(df[f'{keyword}_y'].unique())):
                # if the two notes are different AND the x is none, keep the y
                df.drop([f'{keyword}_x'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])>len([i for i in df[f'{keyword}_y'] if i==None])):
                # if the two notes are different AND the x is none, keep the y
                df.drop([f'{keyword}_x'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_y':f'{keyword}'}, inplace=True)

                done=True
                return df

            elif (df[f'{keyword}_x'].equals(df[f'{keyword}_y'])==False) and (len([i for i in df[f'{keyword}_x'] if i==None])<len([i for i in df[f'{keyword}_y'] if i==None])):
                # if the two notes are different AND the y is none, keep the x
                df.drop(['Notes_y'], axis=1, inplace=True)
                df.rename(columns={f'{keyword}_x':f'{keyword}'}, inplace=True)

                done=True
                return df

        else:
            return df

switcher = {
    'tblPlots':no_pk,
    'tblLines':no_pk,
    'tblLPIDetail':lpi_pk ,
    'tblLPIHeader':lpi_pk ,
    'tblGapDetail':gap_pk ,
    'tblGapHeader':gap_pk ,
    # 'tblQualHeader':no_pk ,
    # 'tblQualDetail':no_pk ,
    'tblSoilStabHeader':no_pk ,
    'tblSoilStabDetail':no_pk ,
    'tblSoilPitHorizons':no_pk ,
    'tblSoilPits':no_pk ,
    'tblSpecRichHeader':sperich_pk ,
    'tblSpecRichDetail':sperich_pk ,
    'tblPlantProdHeader':no_pk,
    'tblPlantProdDetail':no_pk,
    # 'tblPlotNotes',
    'tblPlantDenHeader':plantden_pk ,
    'tblPlantDenDetail':plantden_pk ,
    'tblSpecies':no_pk,
    'tblSpeciesGeneric':no_pk,
    'tblSites':no_pk,
    'tblBSNE_Box':bsne_pk ,
    'tblBSNE_BoxCollection':bsne_pk ,
    'tblBSNE_Stack':bsne_pk ,
    'tblBSNE_TrapCollection':bsne_pk
}
tableswitch ={
    'tblPlots':"no pk",
    'tblLines':"no pk",
    'tblLPIDetail':"RecKey",
    'tblLPIHeader':"LineKey",
    'tblGapDetail':"RecKey",
    'tblGapHeader':"LineKey",
    # 'tblQualHeader':no_pk(dimapath,),
    # 'tblQualDetail':no_pk(dimapath),
    'tblSoilStabHeader':"PlotKey",
    'tblSoilStabDetail':"RecKey",
    'tblSoilPitHorizons':"no pk",
    'tblSoilPits':"no pk",
    'tblSpecRichHeader':"LineKey",
    'tblSpecRichDetail':"RecKey",
    'tblPlantProdHeader':"PlotKey",
    'tblPlantProdDetail':"RecKey",
    'tblPlotNotes':"no pk",
    'tblPlantDenHeader':"LineKey",
    'tblPlantDenDetail':"RecKey",
    'tblSpecies':"no pk",
    'tblSpeciesGeneric':"no pk",
    'tblSites':"no pk",
    'tblBSNE_Box':"BoxID",
    'tblBSNE_BoxCollection':"BoxID",
    'tblBSNE_Stack':"PlotKey",
    'tblBSNE_TrapCollection':"StackID"
}
