import logging
import psycopg2

''' 
A program to load processes in memory at NASA statuses harvested from an online db
'''

LOG = logging.getLogger()

def _create_connection (dbname: str) -> psycopg2.extensions.connection:
    ''' open connection to local postgresql server instance '''
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def _load_process_list(dbname : str, procs : list):
    ''' load process status data in the db'''

    conn = _create_connection(dbname)
    cur = conn.cursor()

    for proc in procs:
       if len(proc) > 0:
           pdict = proc[0]
           pname = pdict['Processname']
           fname = pdict['Filename']
           loc = pdict['Location']
           desc = pdict['Description']
           status = pdict['Status']

           # insert file into file_insert_log
           cur.execute(f'''INSERT INTO process_status ("process_name", "file_name", "description", "status", "location") VALUES ('{pname}', '{fname}', '{desc}', '{status}', '{loc}');''')

    conn.commit()


if __name__ == '__main__':

    import argparse
    import json

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='Program process status checker for EDW-Bigfix data')
    ap.add_argument('-n', '--dbname', type=str, default='apps', help='Name of the database with the application list')
    ap.add_argument('-f', '--data', type=str, help='Name of the process status data (json)')

    # parse argv
    opts = ap.parse_args()

    if not opts.data:
        LOG.fatal ("the --data <file> parameter must be specified")
        ap.print_usage()
        exit()

    data = json.load(open(opts.data))

    _load_process_list(opts.dbname, data)

