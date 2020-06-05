
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
            'sqlalchemy':' INTEGER()',
            'pandas':'Int64',
            'pg':'INTEGER'
        }
        date = {
            'slqalchemy':'DATE()',
            'pandas':'datetime64[ns]',
            'pg':'DATE'
        }
        if 'sqlalchemy' in protocol_choice:
            if 'text' in type:
                return text['sqlalchemy']
            elif 'float' in type:
                return float['sqlalchemy']
            elif 'integer' in type:
                return integer['sqlalchemy']
            elif 'date' in type:
                return date['sqlalchemy']
            else:
                print('type not yet implemented')
        elif 'pandas' in protocol_choice:
            if 'text' in type:
                return text['pandas']
            elif 'float' in type:
                return float['pandas']
            elif 'integer' in type:
                return integer['pandas']
            elif 'date' in type:
                return date['pandas']
            else:
                print('type not yet implemented')
        elif 'pg' in protocol_choice:
            if 'text' in type:
                return text['pg']
            elif 'float' in type:
                return float['pg']
            elif 'integer' in type:
                return integer['pg']
            elif 'date' in type:
                return date['pg']
            else:
                print('type not yet implemented')


    else:
        custom = {
            'sqlalchemy':f'{customtype.upper()}({customsize})'
        }
        if 'sqlalchemy' in protocol_choice:
            return custom['sqlalchemy']


def field_parse(prot:str, dictionary:{}):
    """ takes a dictionary with rudimentary field definitions and fieldtype
    protocol, and returns a dictionary with protocol-parsed fields

    """
    return_d = {}
    length_in = len(dictionary)
    try:
        if 'sql' in prot:
            protocol = 'sqlalchemy'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})


        elif 'pandas' in prot:
            protocol = 'pandas'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})
            # if length_in==len(d):
            #     return return_d

        elif 'pg' in prot:
            protocol = 'pg'
            for k,v in dictionary.items():
                return_d.update({k: protocol_typecast(protocol,v)})
            # if length_in==len(d):
            #     return return_d
    except Exception as e:
        print(e)
    finally:
        return return_d

# quiero que me devuelva un diccionario que es.
# lo lea, lo parsee a traves de iters , me devuelva el adecuado
