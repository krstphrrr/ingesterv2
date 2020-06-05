
"""
depending on protocol choice,
the function spits out the right format
"""
from sqlalchemy import TEXT, INTEGER, NUMERIC, VARCHAR, DATE

def protocol_typecast( protocol_choice : str, type : str):
    # customtype = None
    customsize = None
    if 'v_' in type:
        # customtype = 'varchar'
        customsize = int(type.split('_')[1])



    """ dictionary with kv pairs of type-protocol for each field type"""
    text={
        'sqlalchemy':TEXT(),
        'pandas':"object",
        'pg': "TEXT",
        "custom" : VARCHAR(customsize),
        "custompg": f'VARCHAR({customsize})'
    }
    float = {
        'sqlalchemy' : 'NUMERIC()',
        'pandas' : 'float64',
        'pg' : "NUMERIC"
    }
    integer = {
        'sqlalchemy' : 'INTEGER()',
        'pandas' : 'Int64',
        'pg' : 'INTEGER'
    }
    date = {
        'sqlalchemy' : 'DATE()',
        'pandas' : 'datetime64[ns]',
        'pg' : 'DATE'
    }


    """ executed pattern will depend on function parameters """

    if 'sqlalchemy' in protocol_choice:
        if 'text' in type:
            return text['sqlalchemy']
        elif 'float' in type:
            return float['sqlalchemy']
        elif 'integer' in type:
            return integer['sqlalchemy']
        elif 'date' in type:
            return date['sqlalchemy']
        elif 'v_' in type:
            return text['custom']
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
        elif 'v_' in type:
            return text['pandas']
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
        elif 'v_' in type:
            return text['custompg']
        else:
            print('type not yet implemented')

def field_parse(prot:str, dictionary:{}):
    """ takes a dictionary with rudimentary field definitions and fieldtype
    protocol, and returns a dictionary with protocol-parsed fields
    it understands:

    - 'text', 'float', 'integer', 'date', and 'v_*NUMBER*' for varchar where
    *NUMBER* is the size of the varchar field,

    """
    return_d = {}

    try:
        if 'sql' in prot:
            protocol = 'sqlalchemy'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})

        elif 'pandas' in prot:
            protocol = 'pandas'
            for k,v in dictionary.items():
                return_d.update({k:protocol_typecast(protocol,v)})

        elif 'pg' in prot:
            protocol = 'pg'
            for k,v in dictionary.items():
                return_d.update({k: protocol_typecast(protocol,v)})

    except Exception as e:
        print(e)
    finally:
        return return_d


def sql_command(typedict, name):
    inner_list = [f"\"{k}\" {v}" for k,v in typedict.items()]
    part_1 = f""" CREATE TABLE gisdb.public.\"{name}\" ("""
    try:
        for i,x in enumerate(inner_list):
            if i==len(inner_list)-1:
                part_1+=f"{x}"
            else:
                part_1+=f"{x},"
    except Exception as e:
        print(e)
    finally:
        part_1+=");"
        return part_1
