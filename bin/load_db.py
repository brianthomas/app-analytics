'''
Load Application CSV files, software- and process-oriented, into a postgresql database.
'''
import psycopg2
import pandas as pd
import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def create_connection (dbname):
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def parse(file: str) -> pd.DataFrame:

    df = None
    if file.find('gz') == -1:
        df = pd.read_csv(file, encoding="cp437", skiprows=[0], skipfooter=1, engine='python') 
    else:
        df = pd.read_csv(file, compression='gzip', encoding="cp437", skiprows=[0], skipfooter=1, engine='python') 

    return df

if __name__ == '__main__':
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='XLSM requirements db loader')
    ap.add_argument('-n', '--dbname', type=str, default = 'apps')
    ap.add_argument('-s', '--software', type=str, help='Name of the file which holds the software data')
    ap.add_argument('-p', '--process',  type=str, help='Name of the file which holds the process data')

    # parse argv
    opts = ap.parse_args()

    if not opts.process:
        print ("the --process <file> parameter must be specified")
        ap.print_usage()
        exit()

    if not opts.software:
        print ("the --software <file> parameter must be specified")
        ap.print_usage()
        exit()

    # parse out out data into in-memory DataFrames
    process_data = parse(opts.process)
    software_data = parse(opts.software)

    # load the data

    # print back to STDOUT


