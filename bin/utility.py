
import psycopg2
import pandas as pd

def create_connection (dbname):
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def create_center_map(dbname):
    """
    Returns a dictionary
    values: set of indices for center_id that correspond
    """

    conn = create_connection(dbname)
    cur = conn.cursor()
    cur.execute('SELECT rowid, center FROM center_map;')
    center_names = cur.fetchall() 

    center_map = {}
    for index, center in center_names:
        center_map[index] = center

    return center_map


def create_os_bin_map(dbname):
    """
    Returns a dictionary
    NOTE: this hardcodes "other" as RHEL or CentOS
    which is true for the 6/26/17 dataset
    key: grouped names
    values: set of indices for os_id that correspond
    """
    conn = create_connection(dbname)
    cur = conn.cursor()
    cur.execute('SELECT rowid, operating_system FROM os_map;')
    os_names = cur.fetchall()
    os_name_map = {'Microsoft Windows 7': set(), 'Microsoft Windows 8': set(),\
                'Microsoft Windows 10': set(), 'Microsoft Windows Server': set(),\
                'Other Windows': set(), 'Mac OS X': set(), 'OS Unknown': set(),\
                'RHEL or CentOS': set()}
    cur.close()
    conn.close()
    for index, os_name in os_names:
        if 'Microsoft Windows 7' in os_name:
            os_name_map['Microsoft Windows 7'].add(index)
        elif 'Microsoft Windows 8' in os_name:
            os_name_map['Microsoft Windows 8'].add(index)
        elif 'Microsoft Windows 10' in os_name:
            os_name_map['Microsoft Windows 10'].add(index)
        elif 'Microsoft Windows Server' in os_name:
            os_name_map['Microsoft Windows Server'].add(index)
        elif 'Windows' in os_name:
            os_name_map['Other Windows'].add(index)
        elif 'Mac OS X' in os_name:
            os_name_map['Mac OS X'].add(index)
        elif 'OS Unknown' in os_name:
            os_name_map['OS Unknown'].add(index)
        else:
            os_name_map['RHEL or CentOS'].add(index)

    return os_name_map

class Query:

    @property
    def result (self): 
        return self._result

    @staticmethod
    def _do_query (conn, tablename, constraint):
        query_str = f"""SELECT * FROM {tablename} {constraint};"""
        return pd.read_sql_query(query_str, conn)

    def __init__(self, conn, tablename, constraint=""):
        self._result = Query._do_query(conn, tablename, constraint)


class HistoQuery:

    @property
    def result (self):
        return self._result

    @staticmethod
    def _do_query (conn, tablename, column, constraint):
        query_str = f"""SELECT {column}, count(*) as num FROM {tablename} {constraint} group by {column} order by num desc;"""
        return pd.read_sql_query(query_str, conn)

    def __init__(self, conn, tablename, column, constraint=""):
        self._result = HistoQuery._do_query(conn, tablename, column, constraint)

