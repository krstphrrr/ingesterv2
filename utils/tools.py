from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool

def config(filename='utils/database.ini', section='postgresql'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db

def geoconfig(filename='utils/database.ini', section='geo'):
    """
    Same as config but reads another section.
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))
    return db


class db:
    params = config()
    # str = connect(**params)
    str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**params)
    str = str_1.getconn()

    def __init__(self):

        self._conn = connect(**params)
        self._cur= self._conn.cursor()
