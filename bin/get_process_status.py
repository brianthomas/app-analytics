from bs4 import BeautifulSoup
import time
import logging
import requests
import psycopg2

''' 
A program to harvest processes in memory at NASA and check their status against an online database
'''

LOG = logging.getLogger()

SEARCH_URL = "https://www.systemlookup.com/search.php?list=&type=filename&s=&search="

PAGE_LOAD_WAIT_TIME = 1

def _create_connection (dbname: str) -> psycopg2.extensions.connection:
    ''' open connection to local postgresql server instance '''
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def _check (search_url: str) -> list:

    data = [] 

    # grab page
    get_request = requests.get(search_url)

    # now make parsable 
    soup=BeautifulSoup(get_request.content, "lxml")

    # now parse out the table in the page with info we want
    tables = soup.find_all('table')
    for table in tables:
        tid = table.get('class')

        # check for the right table which has our results
        if tid != None and tid[0] == "resultsTables":

            # parse out result metadata and store in mem
            fieldnames = []
            for row in table.find_all('tr'):
                # first row initializes fieldnames
                if len(fieldnames) == 0: 
                    for item in row.find_all('td'):
                        fieldnames.append(item.string)
                else:
                    count = 0
                    result = {}
                    for item in row.find_all('td'):
                        fieldname = fieldnames[count]
                        if fieldname == 'Name':
                            pass
                        else:
                            result[fieldname] = item.string
                        count = count + 1
                    data.append(result)
  
            break

    # return results
    return data

def _get_process_list(dbname : str) -> set:
    ''' get a list of processes in memory '''

    conn = _create_connection(dbname)
    cur = conn.cursor()

    cur.execute("select distinct process_name from device_process;")
    query = cur.fetchall()
    apps = set([i[0] for i in query])

    return apps


if __name__ == '__main__':

    import argparse
    import json

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='Program process status checker for EDW-Bigfix data')
    ap.add_argument('-n', '--dbname', type=str, default='apps', help='Name of the database with the application list')

    # parse argv
    opts = ap.parse_args()

    results = []
    for processname in _get_process_list(opts.dbname): 
        results.append(_check (SEARCH_URL+processname))

    print (json.dumps(results))
    #print (results)

