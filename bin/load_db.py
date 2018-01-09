'''
Load Application CSV files, software- and process-oriented, into a postgresql database.
'''
import psycopg2
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def _create_connection (dbname: str) -> psycopg2.extensions.connection:
    ''' open connection to local postgresql server instance '''
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def _checkmaps(conn: psycopg2.extensions.connection, col: str, tbl: str, file_itms: set):
    ''' check and remap data '''

    cur = conn.cursor()
    col = col.replace (" ", "_")
    col = col.lower()

    LOG.debug(f'''SELECT DISTINCT "{col}" from {tbl};''')
    cur.execute(f'''SELECT DISTINCT "{col}" from {tbl};''')
    query = cur.fetchall()
    existing_items = set([i[0] for i in query])

    for each in file_itms:
        if each not in existing_items:
            cur.execute(f'''INSERT INTO {tbl} ("{col}") VALUES ('{each}');''')
            LOG.debug(f"Inserted new item {col}: {each} into table {tbl}")

    conn.commit()

def _insert_process_files(dbname: str, fname: str):

    LOG.info("Writing {0} to database".format(fname))

    # parse out out data into in-memory DataFrames
    data = _parse(fname)

    conn = _create_connection(dbname)
    cur = conn.cursor()

    LOG.info ("Doing mapping checks")
    mapping_check = {
            "Center Name": "center_map",\
            "Device Name": "device_map",\
            "Process Path": "process_map",\
#            "Operating System": "os_map",\
#            "OS Version": "os_version_map",\
                    }

    for col, tbl in mapping_check.items():
        _checkmaps(conn, col, tbl, set(data[col].unique()) )

    # remap center to an id
    cur.execute('select rowid, center_name from center_map;')
    center_dict = {k:v for v, k in cur.fetchall()}

    # remap device to an id
    cur.execute('select rowid, device_name from device_map;')
    device_dict = {k:v for v, k in cur.fetchall()}

    # remap process to an id
    cur.execute('select rowid, process_path from process_map;')
    process_dict = {k:v for v, k in cur.fetchall()}

    col_replace = {
                    'Center Name': 'center_name',\
                    'Device Name': 'device_name',\
                    'Process Name': 'process_name',\
                    'Process Path': 'process_path',\
                    'Operating System': 'os_id',\
                    'OS Version': 'os_version_id',\
                    'Is Server': 'is_server',\
                    'Is Virtual':'is_virtual',\
                    'Is Machine Internet Accessible': 'internet_accessible'
                  }

    LOG.info("Fixing data -- invoking remap")
    data['center_id'] = data['Center Name'].replace(center_dict)
    data['device_id'] = data['Device Name'].replace(device_dict)
    data['process_id'] = data['Process Path'].replace(process_dict)
    #data['os_id'] = data['Operating System'].replace(os_dict)
    #data['os_version_id'] = data['OS Version'].replace(os_version_dict)

    # do the rename
    data.rename(columns = col_replace, inplace=True)

    LOG.info("data slicing")
    device_data = data[["device_id", "center_id", "is_server",\
#                        "is_virtual", "os_id", "os_version_id",\
               ]].drop_duplicates() 
    process_data = data[["process_id", "process_name", "process_path"]].drop_duplicates() 
    LOG.debug(process_data)

    dev_proc_data = data[["process_id", "device_id"]].drop_duplicates() 

    LOG.debug(device_data)
    LOG.debug(dev_proc_data)

    # build the tables now
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres@localhost:5432/'+dbname)

    # update device metadata 
    LOG.info("update device metadata")
    device_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE device_map AS f" + \
          " SET center_id = t.center_id," +\
          " is_server = t.is_server" + \
          " FROM temp_table AS t" + \
          " WHERE f.rowid = t.device_id"
    cur.execute(sql)
    conn.commit()

    # update process metadata 
    LOG.info("update process metadata")
    process_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE process_map AS f" + \
          " SET process_name = t.process_name" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = t.process_id"
    cur.execute(sql)

    # add new associations
    LOG.info("add new dev/process associations")
    dev_proc_data.to_sql('device_process_assoc', engine, if_exists='append', index=False)

    # insert file into file_insert_log
    cur.execute(f'''INSERT INTO file_insert_log (filename) VALUES ('{fname}');''')

    conn.commit()
    conn.close()

def _insert_software_files(dbname: str, fname: str):

    # parse out out data into in-memory DataFrames
    data = _parse(fname)

    LOG.info("Writing {0} to database".format(fname))

    conn = _create_connection(dbname)
    cur = conn.cursor()

    LOG.info ("Doing mapping checks")
    mapping_check = {
            "Center Name": "center_map",\
            "Device Name": "device_map",\
            "Install Location": "software_map",\
#           "Operating System": "os_map",\
#           "OS Version": "os_version_map",\
                    }

    for col, tbl in mapping_check.items():
        _checkmaps(conn, col, tbl, set(data[col].unique()) )

    # remap center to an id
    cur.execute('select rowid, center_name from center_map;')
    center_dict = {k:v for v, k in cur.fetchall()}

    # remap device to an id
    cur.execute('select rowid, device_name from device_map;')
    device_dict = {k:v for v, k in cur.fetchall()}

    # remap software location to an id
    cur.execute('select rowid, install_location from software_map;')
    software_dict = {k:v for v, k in cur.fetchall()}

    col_replace = {
                    'Center Name': 'center_name',\
                    'Device Name': 'device_name',\
                    'Software Name': 'software_name',\
                    'Install Location': 'install_location',\
                    'Operating System': 'os_id',\
                    'OS Version': 'os_version_id',\
                    'Is Server': 'is_server',\
                    'Is Virtual':'is_virtual',\
                    'Is Machine Internet Accessible': 'internet_accessible'
                  }

    LOG.info("Fixing software data -- invoking remap")
    data['center_id'] = data['Center Name'].replace(center_dict)
    data['device_id'] = data['Device Name'].replace(device_dict)
    data['software_id'] = data['Install Location'].replace(software_dict)
    #data['os_id'] = data['Operating System'].replace(os_dict)
    #data['os_version_id'] = data['OS Version'].replace(os_version_dict)

    # do the rename
    data.rename(columns = col_replace, inplace=True)

    LOG.info("data slicing")
    device_data = data[["device_id", "center_id", "is_server",\
#                        "is_virtual", "os_id", "os_version_id",\
               ]].drop_duplicates()
    LOG.debug(device_data)

    software_data = data[["software_id", "software_name", "install_location"]].drop_duplicates()
    LOG.debug(software_data)
    print(software_data)

    dev_software_data = data[["software_id", "device_id"]].drop_duplicates()
    LOG.debug(dev_software_data)

    # build the tables now
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres@localhost:5432/'+dbname)

    # update device metadata
    LOG.info("update device metadata")
    device_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE device_map AS f" + \
          " SET center_id = t.center_id," +\
          " is_server = t.is_server" + \
          " FROM temp_table AS t" + \
          " WHERE f.rowid = t.device_id"
    cur.execute(sql)
    conn.commit()

    # update software metadata
    software_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE software_map AS f" + \
          " SET software_name = t.software_name" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = (t.software_id)::integer"
    cur.execute(sql)

    # add new associations
    LOG.info("add new dev/process associations")
    dev_software_data.to_sql('device_process_assoc', engine, if_exists='append', index=False)

    # insert file into file_insert_log
    cur.execute(f'''INSERT INTO file_insert_log (filename) VALUES ('{fname}');''')

    conn.commit()
    conn.close()

def _parse(file: str) -> pd.DataFrame:

    df = None
    if file.find('gz') == -1:
        df = pd.read_csv(file, encoding="cp437", skiprows=[0], skipfooter=1, engine='python') 
    else:
        df = pd.read_csv(file, compression='gzip', encoding="cp437", skiprows=[0], skipfooter=1, engine='python') 

    # clean up data a little
    for col in df: 
        print ("Cleaning column: "+col)
        df [col] = df[col].replace("\'", "");

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
        LOG.fatal ("the --process <file> parameter must be specified")
        ap.print_usage()
        exit()

    if not opts.software:
        LOG.fatal ("the --software <file> parameter must be specified")
        ap.print_usage()
        exit()

    # load the data
    #_insert_process_files (opts.dbname, opts.process)
    _insert_software_files (opts.dbname, opts.software)

    LOG.info ("finished")

