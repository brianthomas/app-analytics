
import bs4
from bs4 import BeautifulSoup
import re
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

def _check (processname: str, processpaths: list) -> list:

    data = list()

    # grab page
    get_request = requests.get(SEARCH_URL+processname)

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
                    result['Processname'] = processname
                    for item in row.find_all('td'):
                        fieldname = fieldnames[count]
                        result[fieldname] = item.text 

                        count = count + 1

                    data.append(_add_location(result))
  
            break

    # return results
    return data

def _add_location(data: dict) -> dict:
    ''' add the location based on regex on the description '''

    data['Location'] = None

    description = data['Description']
    if description != None:
        m = re.search('.+Note: Located in (.+?)$', description)
        if m:
            data['Location'] = m.group(1)

    return data

def _get_process_list(dbname : str) -> set:
    ''' get a list of processes in memory '''

    conn = _create_connection(dbname)
    cur = conn.cursor()

    results = {}

    cur.execute("select distinct process_name, process_path from device_process;")
    query = cur.fetchall()

    for i,j in query:
        if i not in results:
            results[i] = []
        results[i].append(j)

    return results 


if __name__ == '__main__':

    import argparse
    import json

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='Program process status checker for EDW-Bigfix data')
    ap.add_argument('-n', '--dbname', type=str, default='apps', help='Name of the database with the application list')

    # parse argv
    opts = ap.parse_args()

    results = []
    for processname, processpaths in _get_process_list(opts.dbname).items(): 
        results.append(_check (processname, processpaths))

    print (json.dumps(results))
    print (results)

