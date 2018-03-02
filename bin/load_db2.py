'''
Load Application CSV files harvested from BigFix console (rather than EDW), which are software-oriented, into a postgresql database.
'''
import psycopg2
import pandas as pd
import logging

import hashlib

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
            # clean up formatting problems
            if each != None:
                each = each.replace("\'", "") 
            cur.execute(f'''INSERT INTO {tbl} ("{col}") VALUES ('{each}');''')
            LOG.debug(f"Inserted new item {col}: {each} into table {tbl}")

    conn.commit()

def _insert_software_files(dbname: str, fname: str):

    # parse out out data into in-memory DataFrames
    data = _parse(fname)

    LOG.info("Writing {0} to database".format(fname))

    conn = _create_connection(dbname)
    cur = conn.cursor()

    LOG.info ("Doing mapping checks")
    mapping_check = {
            "_HomeCenter": "center_map",\
            "Computer Name": "device_map",\
            "software_hash": "software_map",\
            "IP Address": "ip_map",\
            "OS": "os_map",\
                    }

    col_replace = {
                    '_HomeCenter': 'center_name',\
                    'Computer Name': 'device_name',\
                    'Software Name': 'software_name',\
                    'Version': 'software_version',\
                    'software_hash': 'software_hash',\
                    'Install Location': 'install_location',\
                    'IP Address': 'ip_address',\
                    'CPU': 'cpu',\
                    'CPUS': 'cpu_cores',\
                    'Last Report Time': 'report_time',\
                    'Device Type': 'device_type',\
                    'OS': 'os_name'
                   # 'Is Server': 'is_server',\
                   # 'Is Virtual':'is_virtual',\
                   # 'Is Machine Internet Accessible': 'internet_accessible'
                  }

    for coln, tbl in mapping_check.items():
        col = col_replace[coln]
        _checkmaps(conn, col, tbl, set(data[coln].unique()) )

    # remap center to an id
    cur.execute('select rowid, center_name from center_map;')
    center_dict = {k:v for v, k in cur.fetchall()}

    # remap device to an id
    cur.execute('select rowid, device_name from device_map;')
    device_dict = {k:v for v, k in cur.fetchall()}

    # remap software location to an id
    cur.execute('select rowid, software_hash from software_map;')
    software_dict = {k:v for v, k in cur.fetchall()}

    # remap os to an id
    cur.execute('select rowid, os_name from os_map;')
    os_dict = {k:v for v, k in cur.fetchall()}

    # remap ip to an id
    cur.execute('select rowid, ip_address from ip_map;')
    ip_dict = {k:v for v, k in cur.fetchall()}

    LOG.info("Fixing software data -- invoking remap")
    data['center_id'] = data['_HomeCenter'].replace(center_dict)
    data['device_id'] = data['Computer Name'].replace(device_dict)
    data['software_id'] = data['software_hash'].replace(software_dict)
    data['os_id'] = data['OS'].replace(os_dict)
    data['ip_id'] = data['IP Address'].replace(ip_dict)

    # do the rename
    data.rename(columns = col_replace, inplace=True)

    LOG.info("data slicing")
    device_data = data[["device_id", "center_id", "os_id", "cpu", "cpu_cores", "device_type"
#                        "is_server",\
#                        "is_virtual", 
               ]].drop_duplicates()
    LOG.debug(device_data)

    software_data = data[["software_id", "software_name", "software_version", "install_location"
               ]].drop_duplicates()
    LOG.debug(software_data)

    dev_software_data = data[["software_id", "device_id"]].drop_duplicates()
    LOG.debug(dev_software_data)

    dev_ip_data = data[["ip_id", "device_id", "report_time"]].drop_duplicates()
    LOG.debug(dev_ip_data)

    # build the tables now
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres@localhost:5432/'+dbname)

    # update device metadata
    LOG.info("update device metadata")
    device_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE device_map AS f" + \
          " SET center_id = t.center_id, os_id = t.os_id," +\
          " cpu = t.cpu, cpu_cores = (t.cpu_cores)::integer," +\
          " device_type = t.device_type" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = t.device_id"
    #     ", is_server = t.is_server" +\
    cur.execute(sql)
    conn.commit()

    # update software metadata
    software_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE software_map AS f" + \
          " SET software_name = t.software_name, software_version = t.software_version, install_location = t.install_location" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = (t.software_id)::integer"
    cur.execute(sql)

    # add new associations
    LOG.info("add new dev/process associations")
    dev_software_data.to_sql('device_software_assoc', engine, if_exists='append', index=False)
    dev_ip_data.to_sql('device_ip_assoc', engine, if_exists='append', index=False)

    # insert file into file_insert_log
    cur.execute(f'''INSERT INTO file_insert_log (filename) VALUES ('{fname}');''')

    conn.commit()
    conn.close()

def _software_hash (row):

    hash_string = row['Software Name']

    if row['Version'] != None: 
        hash_string += row['Version']

    if row['Install Location'] != None: 
        hash_string += row['Install Location'] 

    hash_object = hashlib.sha1(hash_string.encode('utf-8'))

    return hash_object.hexdigest()

def _parse(file: str) -> pd.DataFrame:

    # Header of parsed file
    # Computer Name,User Name,Device Type,Number of Processor Cores - Windows,Number of Processor Cores - Mac OS X,Installed Applications,Installed Applications,_HomeCenter,IP Address,OS,CPU,Last Report Time

    special_delim_char = '•';

    df = None
    if file.find('gz') == -1:
        df = pd.read_csv(file, encoding="utf-8", engine='c') 
    else:
        df = pd.read_csv(file, compression='gzip', encoding="utf-8", engine='c') 

    # find which data are Windows, which are Mac OS
    result = df['Installed Applications'] != "DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout" 
    if result[0] == True:
        windows_install_app_col = 'Installed Applications' 
        macos_install_app_col = 'Installed Applications.1' 
    else:
        windows_install_app_col = 'Installed Applications.1' 
        macos_install_app_col = 'Installed Applications' 

    # drop out crap rows (essentially are cut in headers) which are not used/contain no data of use 
    df = df[df[windows_install_app_col] != "DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout"] 
    df = df[df[macos_install_app_col] != "DisplayName • DisplayVersion • CreationTime • InstallLocation"] 

    # split out the Installed Applications column
    df1, df2 = [x for _, x in df.groupby(df['Number of Processor Cores - Mac OS X'] != '<not reported>')]

    # Now, depending on column split out the Installed Applications columns into set of cols w/ no nulls

    # Windows 
    # parse out installed apps : 
    #     DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout
    new_df1 = df1.join(df1[windows_install_app_col].str.split('•', expand=True).rename(columns={0:'Software Name', 1:'Version', 2:'Publisher', 3:'Install Date', 4:'Install Location', 5:'URL'})) 
    new_df1.drop(['Number of Processor Cores - Mac OS X'], axis=1, inplace=True)
    new_df1.rename(index=str, columns={"Number of Processor Cores - Windows": "CPUS"}, inplace=True)

    # Mac OS 
    # parse out installed apps: 
    #     DisplayName • DisplayVersion • CreationTime • InstallLocation
    new_df2 = df2.join(df2[macos_install_app_col].str.split('•', expand=True).rename(columns={0:'Software Name', 1:'Version', 2:'Install Date', 3:'Install Location'})) 
    new_df2.drop(['Number of Processor Cores - Windows'], axis=1, inplace=True)
    new_df2.rename(index=str, columns={"Number of Processor Cores - Mac OS X": "CPUS"}, inplace=True)

    # append them together rowwise
    df = pd.concat([new_df1, new_df2])

    # drop out more crap rows
    # df = df[df['Software Name'] == ""] 

    # add software hash column
    df['software_hash'] = df.apply(_software_hash, axis=1)

    # clean up data a little
    cleaned = df
    for col in df: 
        cleaned [col] = df[col].replace("\'", "");

    #print (cleaned)

    return cleaned

if __name__ == '__main__':
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='XLSM requirements db loader')
    ap.add_argument('-n', '--dbname', type=str, default = 'apps')
    ap.add_argument('-s', '--software', type=str, help='Name of the file which holds the software data')

    # parse argv
    opts = ap.parse_args()

    if not opts.software:
        LOG.fatal ("the --software <file> parameter must be specified")
        ap.print_usage()
        exit()

    # load the data
    _insert_software_files (opts.dbname, opts.software)

    LOG.info ("finished")

