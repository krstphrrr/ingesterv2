import pandas as pd
import numpy as np


def concern(df):
    for field in df.columns[5:27]:
        df[field] = df[field].apply(lambda x: 1 if x=='Y' else (0 if x=='N' else x))
    return df

def disturbance(df):
    addins = ['PrimaryKey','FIPSPSUPNT','DBKey']
    for field in df[[col for col in df.columns if col not in addins]].columns[6:]:
        df[field] = df[field].apply(lambda x: 1 if (x=='Y') else (0 if x=='N' else x)).astype('Int64')
    return df

def ecosite(df):
    pass

def pastureheights(df):
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
