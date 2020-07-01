
"""
depending on protocol choice,
the function spits out the right format
"""
from psycopg2 import sql
from tqdm import tqdm
from io import StringIO
import psycopg2, re, os, os.path, pandas as pd
from sqlalchemy import *
from utils.tools import db
from sqlalchemy import TEXT, INTEGER, NUMERIC, VARCHAR, DATE
from pandas import read_sql_query
from geoalchemy2 import Geometry, WKTElement, WKBElement
from shapely.geometry import Point
import geopandas as gpd

class model_handler:
    engine = None
    initial_dataframe = None
    checked_df = None
    typedict = None
    sqlalchemy_types = None
    pandas_dtypes = None
    psycopg2_types = None
    psycopg2_command = None
    geo_dataframe = None

    def __init__(self,path, name2dictionary, tablename):
        """ needs to match name to model and pull dictionary """

        """ clearing attributes & setting engine """
        self.engine = create_engine(os.environ.get('DBSTR'))
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]

        """ prepping a geodf from path """

        if 'dataHeader' in tablename:
            self.initial_dataframe = pd.read_csv(path, low_memory=False)
            self.geo_dataframe = gpd.GeoDataFrame(
                self.initial_dataframe,
                crs='epsg:4326',
                geometry = [
                    Point(xy) for xy in zip(self.initial_dataframe.Longitude_NAD83,
                    self.initial_dataframe.Latitude_NAD83)
                    ]
                )
            self.geo_dataframe['wkb_geometry'] = self.geo_dataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
            self.geo_dataframe.drop('geometry', axis=1, inplace=True)
            self.checked_df = self.geo_dataframe.copy()
        else:
            self.initial_dataframe = pd.read_csv(path, low_memory=False)
            self.checked_df = self.initial_dataframe.copy()


        """ creating type dictionaries """
        self.typedict = name2dictionary
        self.sqlalchemy_types = field_parse('sqlalchemy', self.typedict)
        self.pandas_dtypes = field_parse('pandas', self.typedict)
        self.psycopg2_types = field_parse('pg', self.typedict)
        self.psycopg2_command = sql_command(self.psycopg2_types,tablename)


    def checked(self):
        """ fieldtype check """
        for i in self.checked_df.columns:
            if self.checked_df[i].dtype!=self.pandas_dtypes[i]:
                self.checked_df[i] = self.typecast(df=self.checked_df,field=i,fieldtype=self.pandas_dtypes[i])

    def typecast(self,df,field,fieldtype):
        data = df
        castfield = data[field].astype(fieldtype)
        return castfield

    def send_to_pg(self):

        self.initial_dataframe.to_sql('dataLPI', self.engine, index=False, dtype=self.sqlalchemy_types)

    def create_empty_table(self):
        con = db.str
        cur = con.cursor()
        try:
            cur.execute(self.psycopg2_command)
            con.commit()
            # cur.execute("selec")
        except Exception as e:
            con = db.str
            cur = con.cursor()
            print(e)

class ingesterv2:

    con = None
    cur = None
    # data pull on init
    __tablenames = []
    __seen = set()

    def __init__(self):
        """ clearing old instances """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.__tablenames = []
        self.__seen = set()

        """ init connection objects """
        self.con = db.str
        self.cur = self.con.cursor()
        """ populate properties """
        self.pull_tablenames()
    def clear(self,var):
        var = None
        return var

    def pull_tablenames(self):
        if self.__tablenames is not None:
            if self.con is not None:

                try:
                    self.cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;""")
                    query_results = self.cur.fetchall()

                    for table in query_results:
                        if table not in self.__seen:
                            self.__seen.add(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                            self.__tablenames.append(re.search(r"\(\'(.*?)\'\,\)",
                            str(table)).group(1))
                except Exception as e:
                    print(e)
                    self.con = db.str
                    self.cursor = self.con.cursor
            else:
                print("connection object not initialized")

    def drop_fk(self, table):

        key_str = "{}_PrimaryKey_fkey".format(str(table))
        print('try: dropping keys...')
        try:
            # print(table)
            self.cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   DROP CONSTRAINT IF EXISTS {1}""").format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            self.con.commit()
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()
        print(f"Foreign keys on {table} dropped")
    def drop_table(self, table):
        try:
            self.cur.execute(
            sql.SQL("DROP TABLE IF EXISTS gisdb.public.{};").format(
            sql.Identifier(table))
            )
            self.con.commit()
            print(table +' dropped')
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()
    def reestablish_fk(self,table):
        key_str = "{}_PrimaryKey_fkey".format(str(table))

        try:

            self.cur.execute(
            sql.SQL("""ALTER TABLE gisdb.public.{0}
                   ADD CONSTRAINT {1}
                   FOREIGN KEY ("PrimaryKey")
                   REFERENCES "dataHeader"("PrimaryKey");
                   """).format(
                   sql.Identifier(table),
                   sql.Identifier(key_str))
            )
            self.con.commit()
        except Exception as e:
            print(e)
            self.con = db.str
            self.cur = self.con.cursor()

    @staticmethod
    def main_ingest( df: pd.DataFrame, table:str,
                    connection: psycopg2.extensions.connection,
                    chunk_size:int = 10000):
                """needs a table first"""
                print(connection)
                cursor = connection.cursor()
                df = df.copy()

                escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
                for col in df.columns:
                    if df.dtypes[col] == 'object':
                        for v, e in escaped.items():
                            df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
                try:
                    for i in tqdm(range(0, df.shape[0], chunk_size)):
                        f = StringIO()
                        chunk = df.iloc[i:(i + chunk_size)]

                        chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                        f.seek(0)
                        cursor.copy_from(f, f'"{table}"', columns=[f'"{i}"' for i in df.columns])
                        connection.commit()
                except psycopg2.Error as e:
                    print(e)
                    connection.rollback()
                cursor.close()

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
        'sqlalchemy' : NUMERIC(),
        'pandas' : 'float64',
        'pg' : "NUMERIC"
    }
    integer = {
        'sqlalchemy' : INTEGER(),
        'pandas' : 'Int64',
        'pg' : 'INTEGER'
    }
    date = {
        'sqlalchemy' : DATE(),
        'pandas' : 'datetime64[ns]',
        'pg' : 'DATE'
    }
    geom = {
        'sqlalchemy' : Geometry('POINT', srid=4326),
        'pandas' : 'geometry',
        'pg' : 'GEOMETRY(POINT, 4326)'
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
