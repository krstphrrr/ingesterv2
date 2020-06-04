
"""
depending on protocol choice,
the function spits out the right format
"""
from sqlalchemy import TEXT, INTEGER, NUMERIC, VARCHAR, DATE
def protocol_typecast(
    protocol_choice:str,
    type:str,
    customtype:str=None,
    customsize:int=None):

    if (customtype is None) and (customsize is None):
        text={
            'sqlalchemy':'TEXT()',
            'pandas':"object",
            'pg': "TEXT",
            "custom":customtype
        }
        float = {
            'sqlalchemy':'NUMERIC()',
            'pandas':'float64',
            'pg':"NUMERIC"
        }
        integer = {
            'sqlalchemy': INTEGER(),
            'pandas':'Int64',
            'pg':'INTEGER'
        }
        date = {
            'slqalchemy':DATE(),
            'pandas':'datetime64[ns]',
            'pg':'DATE'
        }
        if 'sqlalchemy' in protocol_choice:
            if 'text' in type:
                print(text['sqlalchemy'])
    else:
        custom = {
            'sqlalchemy':f'{customtype.upper()}({customsize})'
        }
        if 'sqlalchemy' in protocol_choice:
            return custom['sqlalchemy']

str = 'algo'
str.upper()
protocol_typecast('sqlalchemy','text')
d = {'a':'text', 'b':'int', 'c':'float'}
def field_parse(prot, dictionary):
    return_d = {}
    for k,v in dictionary.items():
        if 'sql' in v:
            print(k, prot, protocol_typecast('sqlalchemy','text'))
field_parse(protocolo, diccionario)
# quiero que me devuelva un diccionario que es.
# lo lea, lo parsee a traves de iters , me devuelva el adecuado
