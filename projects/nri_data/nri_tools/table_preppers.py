import pandas as pd
import numpy as np


def concern(df:pd.DataFrame):
    """ changes to concern table.

    Changes implemented:
        - change all `Y` and `N` across the fields, to 1 and 0.

    -------------------------
    Args:
        df (dataframe)

    -------------------------
    Returns:
        Altered dataframe.

    """
    for field in df.columns[5:27]:
        df[field] = df[field].apply(lambda x: 1 if x=='Y' else (0 if x=='N' else x))
    return df

def disturbance(df:pd.DataFrame):
    """ changes to disturbance table.

    Changes implemented:
        - change all `Y` and `N` across the fields, to 1 and 0.
        - dropped tables not in definitions file.

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    addins = ['PrimaryKey','FIPSPSUPNT','DBKey']
    for field in df[[col for col in df.columns if col not in addins]].columns[6:]:
        df[field] = df[field].apply(lambda x: 1 if (x=='Y') else (0 if x=='N' else x)).astype('Int64')
    # df.drop(columns=['LARGER_MAMMALS','LIVESTOCK_TANKS_PIPELINES','MACHINERY','FENCES','OIL_FIELD_EQUIPMENT'], inplace=True)
    return df

def ecosite(df:pd.DataFrame):
    """ changes to ecosite table.

    Changes implemented:
        - dropped tables not in definitions file.

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    df.drop(columns=['SEQNUM','COVERAGE','START_MARK','END_MARK','ECO_SITE_STATE','ECO_SITE_MLRA','ECO_SITE_LRU','ECO_SITE_NAME'], inplace=True)
    return df

def pastureheights(df:pd.DataFrame):
    """ changes to pastureheights table.

    Changes implemented:
        - changes inches to feet, add wheight, adds unit column, remove
          problematic strings from some numeric columns.

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    height_1 = df[['SURVEY', 'STATE', 'COUNTY', 'PSU', 'POINT', 'TRANSECT', 'DISTANCE','HPLANT','HEIGHT']]
    height_2 = df[['WPLANT', 'WHEIGHT']]
    height_tail = df[['PrimaryKey', 'FIPSPSUPNT', 'DBKey']]

    h2 = height_2.copy()
    h2['preheight2']=df['WHEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                                    (any([y.isdigit() for y in x])==True) and
                                    (any(['+' in z for z in x])!=True) and
                                    ('in' in x.split()) else (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                                        (any([y.isdigit() for y in x])==True) and
                                        (any(['+' in z for z in x])!=True) and
                                        ('ft' in x.split()) else x) )

    h2['preunit2'] = df['WHEIGHT'].apply(lambda x: 'ft' if (any([y.isalpha() for y in x])==True) and
                                               ('in' in x.split()) and
                                               (len(x.split())<=2) else (x.split()[1] if ('ft' in x) and (pd.isnull(x)!=True) else x ) )
    h2['preheight2']=h2['preheight2'].apply(lambda x: x.split()[1].replace('ft',x.split()[0]) if ((isinstance(x,float)!=True)) and (len(x.split())>1) else x)

    h2['preheight2']=h2['preheight2'].apply(lambda x: x.replace('+','') if (isinstance(x,float)!=True) and ('+' in x) else x)

    h2['preheight2']=h2['preheight2'].apply(lambda x: np.nan if (isinstance(x,float)!=True) and ('' in x) and ('0' not in x) and ('61' not in x) else x)

    h2['preheight2']= h2['preheight2'].apply(lambda x: float(x) if (pd.isnull(x)!=True) and (isinstance(x,str)==True) else x )

    h1 = height_1.copy()
    h1['preheight']=df['HEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                                    (any([y.isdigit() for y in x])==True) and
                                    (any(['+' in z for z in x])!=True) and
                                    ('in' in x.split()) else (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                                        (any([y.isdigit() for y in x])==True) and
                                        (any(['+' in z for z in x])!=True) and
                                        ('ft' in x.split()) else x) )

    h1['preunit'] = df['HEIGHT'].apply(lambda x: 'ft' if (any([y.isalpha() for y in x])==True) and
                                               ('in' in x.split()) and
                                               (len(x.split())<=2) else (x.split()[1] if ('ft' in x) and  (pd.isnull(x)!=True) else x ) )

    h1['preheight']=h1['preheight'].apply(lambda x: float(x.split()[0].replace('+','')) if (isinstance(x,float)!=True) and
                                        ('+' in x.split()) else x)
    h1['preheight']=h1['preheight'].apply(lambda x: x.split()[1].replace('ft',x.split()[0]) if ((isinstance(x,float)!=True)) and (len(x.split())>1) else x)

    h1['preheight']=h1['preheight'].apply(lambda x: x.replace('+','') if (isinstance(x,float)!=True) and ('+' in x) else x)

    h1['preheight']=h1['preheight'].apply(lambda x: np.nan if (isinstance(x,float)!=True) and ('' in x) and ('0' not in x) and ('61' not in x) else x)

    h1['preheight']= h1['preheight'].apply(lambda x: float(x) if (pd.isnull(x)!=True) and (isinstance(x,str)==True) else x )


    h1['preunit']=h1['preunit'].apply(lambda x: '' if (x=='0') else x)
    h2['preunit2']=h2['preunit2'].apply(lambda x: '' if (x=='0') else x)

    h1=h1.rename(columns={'HEIGHT':'HEIGHT_OLD'})
    h1=h1.rename(columns={'preheight':'HEIGHT', 'preunit':'HEIGHT_UNIT'})

    h2=h2.rename(columns={'WHEIGHT':'WHEIGHT_OLD'})
    h2=h2.rename(columns={'preheight2':'WHEIGHT', 'preunit2':'WHEIGHT_UNIT'})


    almost= pd.concat([h1,h2,height_tail], axis=1).drop_duplicates()
    almost= almost.drop(columns=['HEIGHT_OLD','WHEIGHT_OLD'])
    almost['WHEIGHT_UNIT'] = 'ft'
    almost['HEIGHT_UNIT'] = 'ft'
    # almost['HPLANT'] = almost['HPLANT'].apply(lambda x: '' if ('None' in x) else x)
    return almost

def soilhorizon(df:pd.DataFrame):
    """ changes to soilhorizon table.

    Changes implemented:
        - add `DEPTH_UNIT` field
        - replaces with numpy nan anything that is not number or letter in the
          fields `HORIZON_TEXTURE`, `TEXTURE_MODIFIER` and `EFFERVESCENCE_CLASS`

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """

    df1 = df.iloc[:,:7]
    df1['DEPTH_UNIT'] = df['DEPTH'].apply(lambda x: 'in' if pd.isnull(x)!=True else x)
    df2 = df.iloc[:,7:]
    dff = pd.concat([df1,df2], axis=1)

    dff['HORIZON_TEXTURE'] = dff['HORIZON_TEXTURE'].apply(lambda x: np.nan if (isinstance(x, float)!=True) and (any([i.isalpha() for i in x])!=True) else x)
    dff['TEXTURE_MODIFIER'] = dff['TEXTURE_MODIFIER'].apply(lambda x: np.nan if (isinstance(x, float)!=True) and (any([i.isalpha() for i in x])!=True) else x)
    dff['EFFERVESCENCE_CLASS'] = dff['EFFERVESCENCE_CLASS'].apply(lambda x: np.nan if (isinstance(x, float)!=True) and (any([i.isalpha() for i in x])!=True) else x)

    return dff

def statenm(df:pd.DataFrame):
    """ changes to statenm table.

    Changes implemented:
        - create a new field `PastureRegion` that assigns a region to a set of states.
        - removes duplicates

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """

    midwest = ['IL', 'IN','IA','MI','MN','MO','OH','WI']
    northeast = ['CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT', 'WV']
    nplains = ['CO','KS', 'MT', 'NE', 'ND','SD','WY']
    scentral = ['AR','LA','OK','TX']
    seast = ['AL','FL','GA','KY', 'MS','NC','SC','TN','VA']
    west = ['AZ', 'CA', 'ID', 'NV', 'NM', 'OR','UT','WA']

    def chooser(state:str):
        if state in midwest:
            return 'Midwest'
        elif state in northeast:
            return 'North East'
        elif state in nplains:
            return 'Northern Plains'
        elif state in scentral:
            return 'South Central'
        elif state in seast:
            return 'South East'
        elif state in west:
            return 'West'

    df['PastureRegion'] = '0'
    df['PastureRegion'] = statenm['STABBR'].apply(lambda x: chooser(x))

    return df.drop_duplicates()

def pintercept(df:pd.DataFrame):
    """ changes to pintercept table.

    Changes implemented:
        - change all the `None` strings for numpy nan for the fields `HIT1`,
          `HIT2`, `HIT3`, `HIT4`, `HIT5`, `HIT6` and `BASAL`.

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    df['HIT1'] = df['HIT1'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['HIT2'] = df['HIT2'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['HIT3'] = df['HIT3'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['HIT4'] = df['HIT4'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['HIT5'] = df['HIT5'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['HIT6'] = df['HIT6'].apply(lambda x: np.nan if ('None' in x ) else x)
    df['BASAL'] = df['BASAL'].apply(lambda x: np.nan if ('None' in x ) else x)
    return df


def practice(df:pd.DataFrame):
    """ changes to practice table.

    Changes implemented:
        - merged `P528A` with `P528`
        - merged `N528A` with `N528`
        - removed `P528A` and `N528A`
        - changed all `Y` and `N` to 1 and 0, and changed the pandas
          columns dtype to integer.

    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    mask = pd.isnull(df.P528A)!=True
    mask2 = pd.isnull(df.N528A)!=True

    df.loc[df.P528A.isnull()!=True,'P528'] = df.P528A[mask]
    df.loc[df.N528A.isnull()!=True,'N528'] = df.N528A[mask2]

    df.drop(columns=['N528A','P528A'], inplace=True)

    addins = ['PrimaryKey','FIPSPSUPNT','DBKey']
    for field in [i for i in df.columns[5:] if i not in addins ]:
        df[field] = df[field].apply(lambda x: np.nan if (isinstance(x,float)!=True) and x!='N' and x!='Y'  else x)
    for field in [i for i in df.columns[5:] if i not in addins ]:
        df[field] = df[field].apply(lambda x: 1 if x=='Y' else (0 if x=='N' else x) )
    for field in [i for i in df.columns[5:] if i not in addins ]:
        df[field] = df[field].astype("Int64")

    return df



def point(df:pd.DataFrame):
    """ changes to point table.

    Changes implemented:
        - removed 'ESD_FITS_SOIL','ACTIVE_CUTTING'
        
    -------------------------
    Args:
        df (dataframe): dataframe to be changed.

    -------------------------
    Returns:
        Altered dataframe.

    """
    # df.drop(columns=['ESD_FITS_SOIL','ACTIVE_CUTTING'],inplace=True)
    # return df
    return df
