'''
Load Application CSV files harvested from BigFix console (rather than EDW), which are software-oriented, into a postgresql database.
'''
import psycopg2
import pandas as pd
import logging
import time
import hashlib

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

COL_REPLACE = {
                    'HomeCenter': 'center_name',\
                    'Computer Name': 'device_name',\
                    'Software Name': 'software_name',\
                    'Version': 'software_version',\
                    'software_hash': 'software_hash',\
                    'Install Location': 'install_location',\
                    'IP Address': 'ip_address',\
                    'CPU': 'cpu',\
                    'CPUS': 'cpu_cores',\
                   # 'Last Report Time': 'report_time',\
                    'Device Type': 'device_type',\
                    'OS': 'os_name'
                   # 'Is Server': 'is_server',\
                   # 'Is Virtual':'is_virtual',\
                   # 'Is Machine Internet Accessible': 'internet_accessible'
              }

def _create_connection (dbname: str) -> psycopg2.extensions.connection:
    ''' open connection to local postgresql server instance '''
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def _checkmaps(conn: psycopg2.extensions.connection, col: str, tbl: str, file_itms: set):
    ''' We do this to resync new data with what is in the db currently. check and remap data '''

    cur = conn.cursor()
    col = col.replace (" ", "_")
    col = col.lower()

    # pulling the 'key' column from the db 
    LOG.debug(f'''SELECT DISTINCT "{col}" from {tbl};''')
    cur.execute(f'''SELECT DISTINCT "{col}" from {tbl};''')
    query = cur.fetchall()
    existing_items = set([i[0] for i in query])

    for each in file_itms:
        #print(str(type(each)))
        #print(each)
        if str(each) not in existing_items:
            # clean up formatting problems
            if each != None and type(each) == str:
                each = each.replace("\'", "") 
            # now insert only the new items we didnt have before into the db
            cur.execute(f'''INSERT INTO {tbl} ("{col}") VALUES ('{each}');''')
            LOG.debug(f"Inserted new item {col}: {each} into table {tbl}")

    conn.commit()

def _update_assocs(conn: psycopg2.extensions.connection, engine, tbl: str, columns: list, values: pd.DataFrame):

    # do the update
    values.to_sql(tbl, engine, if_exists='append', index=False)


def _insert_csv (dbname: str, fname: str, rowsize: int) -> None:

    from io import StringIO

    LOG.info("Inserting data from CSV file: {0}".format(fname))

    # clean the data of nulls
    content = ""
    with open(fname, encoding='utf-8-sig',  errors="backslashreplace") as csvfile:
        while True:
            line = csvfile.readline().replace('\000', '')
            content += line 
            if not line:
                break

    parse_content = StringIO(content)

    #reader = pd.read_csv(parse_content, chunksize = rowsize, compression='gzip', encoding="utf-8", engine='c', dtype='str', low_memory=True)
    reader = pd.read_csv(parse_content, chunksize = rowsize, encoding="utf-8", engine='c', dtype='str', low_memory=True)

    for chunk in reader:
        start = time.clock()
        _insert_df (dbname, chunk)
        LOG.info("Wallclock time of chunk: "+str(time.clock()-start)+" s")

    # insert file into file_insert_log
    conn = _create_connection(dbname)
    cur = conn.cursor()
    cur.execute(f'''INSERT INTO file_insert_log (filename) VALUES ('{fname}');''')
    conn.commit()
    conn.close()

def _insert_df (dbname: str, df: pd.DataFrame) -> None:
    ''' insert a dataframe into the database '''

    data = _clean_df(df)

    LOG.info("Create DB connection")
    conn = _create_connection(dbname)

    # update database map tables with new entires in our data
    _update_db_maps(conn, data)

    master_dict = _build_dictionaries(conn)  

    LOG.info("Fixing data -- building ids for centers")
    data['center_id'] = data['HomeCenter'].map(master_dict['center'])

    LOG.info("Fixing data -- building ids for devices")
    data['device_id'] = data['Computer Name'].map(master_dict['device'])

    LOG.info("Fixing data -- building ids for software")
    data['software_id'] = data['software_hash'].map(master_dict['software'])

    LOG.info("Fixing data -- building ids for os")
    data['os_id'] = data['OS'].map(master_dict['os'])

    #LOG.info("Fixing data -- building ids for ip address")
    #data['ip_id'] = data['IP Address'].map(ip_dict)

    # do the rename
    LOG.info("Fixing data -- rename columns")
    data.rename(columns = COL_REPLACE, inplace=True)

    LOG.info("Data slicing -- device data")
    device_data = data[["device_id", "center_id", "os_id", "cpu", "cpu_cores", "device_type"
#                        "is_server", "is_virtual", 
               ]].drop_duplicates()

    LOG.info("Data slicing -- software data")
    software_data = data[["software_id", "software_name", "software_version", "install_location"
               ]].drop_duplicates()

    #dev_ip_data = data[["ip_id", "device_id", "report_time"]].drop_duplicates()

    # build the tables now
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres@localhost:5432/'+dbname)

    cur = conn.cursor()

    # update device metadata
    LOG.info("Loading -- update device metadata")
    device_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE device_map AS f" + \
          " SET center_id = t.center_id, os_id = t.os_id," +\
          " cpu = t.cpu, cpu_cores = (t.cpu_cores)::integer," +\
          " device_type = t.device_type" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = t.device_id"
    #     ", is_server = t.is_server" +\
    #print (sql)
    cur.execute(sql)
    conn.commit()

    # update software metadata
    LOG.info("Loading -- update software metadata")
    software_data.to_sql('temp_table', engine, if_exists='replace', index=False)
    sql = "UPDATE software_map AS f" + \
          " SET software_name = t.software_name, software_version = t.software_version, install_location = t.install_location" +\
          " FROM temp_table AS t" + \
          " WHERE f.rowid = (t.software_id)::integer"
    cur.execute(sql)

    # add new associations
    LOG.info("Data slicing -- dev - software data")
    LOG.info("Loading -- add new dev/process associations")
    dev_software_data = data[["software_id", "device_id"]].drop_duplicates()
    _update_assocs(conn, engine, 'device_software_assoc', ['software_id', 'device_id'], dev_software_data)

    conn.commit()
    conn.close()


def _build_dictionaries (conn: psycopg2.extensions.connection) -> dict:

    cur = conn.cursor()
    master_dict = {}

    # remap center to an id
    cur.execute('select rowid, center_name from center_map;')
    master_dict['center'] = {k:v for v, k in cur.fetchall()}

    # remap device to an id
    cur.execute('select rowid, device_name from device_map;')
    master_dict['device'] = {k:v for v, k in cur.fetchall()}

    # remap software location to an id
    cur.execute('select rowid, software_hash from software_map;')
    master_dict['software'] = {k:v for v, k in cur.fetchall()}

    # remap os to an id
    cur.execute('select rowid, os_name from os_map;')
    master_dict['os'] = {k:v for v, k in cur.fetchall()}

    # remap ip to an id
    #cur.execute('select rowid, ip_address from ip_map;')
    #master_dict['ip'] = {k:v for v, k in cur.fetchall()}

    return master_dict


def _software_hash (row):

    hash_string = row['Software Name']

    if 'Version' in row and row['Version'] != None: 
        hash_string += str(row['Version']) 

    if 'Install Location' in row and row['Install Location'] != None: 
        hash_string += str(row['Install Location']) 

    hash_object = hashlib.sha1(hash_string.encode('utf-8'))

    return hash_object.hexdigest()

def _clean_df (df: pd.DataFrame) -> pd.DataFrame:
    ''' rearrange and clean data in the dataframe '''

    LOG.info("Cleaning DataFrame")
    LOG.debug("  Cols : "+ str(df.columns.values.tolist()))
    LOG.info("  Cols : "+ str(df.dtypes))

    # Header of parsed file
    # Computer Name,User Name,Device Type,Number of Processor Cores - Windows,Number of Processor Cores - Mac OS X,Installed Applications,Installed Applications.1,HomeCenter,IP Address,OS,CPU,Last Report Time

    special_delim_char = '•';

    # print(df['Installed Applications'])

    # find which data are Windows, which are Mac OS
    # and set the installed app columns appropriately
    #
    if "DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout" in df['Installed Applications']:
        windows_install_app_col = 'Installed Applications' 
        macos_install_app_col = 'Installed Applications.1' 
    else:
        windows_install_app_col = 'Installed Applications.1' 
        macos_install_app_col = 'Installed Applications' 

    LOG.info("Drop out cut in head rows")
    # drop out crap rows (essentially are cut in headers) which are not used/contain no data of use 
    df = df[df[windows_install_app_col] != "DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout"] 
    df = df[df[macos_install_app_col] != "DisplayName • DisplayVersion • CreationTime • InstallLocation"] 

    LOG.info("Split dataframe into windows and macosx")
    # split out the Processor Cores column
    win_df = None 
    macos_df = None
    try:
        # print(df)
        win_df, macos_df = [x for _, x in df.groupby(df['Number of Processor Cores - Windows'] == '<not reported>')]

    except ValueError: 

        LOG.debug("Got ValueError on Split, no win or mac machines?")
        # there are no windows or MacOS machines in the dataset, filter for which
        check = df['Number of Processor Cores - Windows']
        if len(check == '<not reported>') > 0:
            # its macos
            win_df = None
            macos_df = df
        else:
            # its windows
            win_df = df
            macos_df = None

    # Now, depending on column split out the Installed Applications columns into set of cols w/ no nulls

    if win_df is not None:
        # Windows 
        # parse out installed apps : 
        #     DisplayName • DisplayVersion • Publisher • InstallDate • InstallLocation • URLInfoAbout

        LOG.info("Parse windows installed apps using column: "+windows_install_app_col)

        new_win_df = win_df.join( win_df[windows_install_app_col].str.split('•', expand=True).rename(columns={0:'Software Name', 1:'Version', 2:'Publisher', 3:'Install Date', 4:'Install Location', 5:'URL'})) 
        new_win_df.drop(['Number of Processor Cores - Mac OS X'], axis=1, inplace=True)
        new_win_df.drop(['Installed Applications', 'Installed Applications.1'], axis=1, inplace=True)
        new_win_df.rename(index=str, columns={"Number of Processor Cores - Windows": "CPUS"}, inplace=True)

        # print (new_win_df)

    if macos_df is not None:
        # Mac OS 
        # parse out installed apps: 
        #     DisplayName • DisplayVersion • CreationTime • InstallLocation

        LOG.info("Parse macos installed apps")

        new_macos_df = macos_df.join(macos_df[macos_install_app_col].str.split('•', expand=True).rename(columns={0:'Software Name', 1:'Version', 2:'Install Date', 3:'Install Location'})) 
        new_macos_df.drop(['Number of Processor Cores - Windows'], axis=1, inplace=True)
        new_macos_df.drop(['Installed Applications', 'Installed Applications.1'], axis=1, inplace=True)
        new_macos_df.rename(index=str, columns={"Number of Processor Cores - Mac OS X": "CPUS"}, inplace=True)

    LOG.info("Merge back windows and macosx data frames into one")
    if macos_df is not None and win_df is not None:
        # append them together rowwise
        df = pd.concat([new_win_df, new_macos_df])
    elif macos_df is not None:
        df = new_macos_df
    elif win_df is not None:
        df = new_win_df
    else:
        LOG.fatal("?? what happened NO macos or win DF data to clean...error!")

    # add any missing critical columns
    for needed_column in ['Version', 'Install Location']:
        if needed_column not in df:
            df[needed_column] = pd.Series([None for x in range(len(df.index))]) 

    # drop out unused columns
    for unused_column in ['Publisher', 'URL', 'User Name', 'Last Report Time']:
        try:
            df.drop([unused_column], axis=1, inplace=True)
        except ValueError:
            # means column didnt exist, we can ignore
            pass

    LOG.info("Clean up unknown values in CPUS column")
    df['CPUS'].replace(to_replace='<not reported>', value=-1, inplace=True)

    LOG.info("Add software hash column")
    # add software hash column
    df['software_hash'] = df.apply(_software_hash, axis=1)

    LOG.info("Clean out single, double quotes from data")
    # clean up data a little
    for col in df: 
        if df[col].dtype != 'int64':
            df[col] = df[col].str.replace(r"[\"\',]", "")

    #print (df)

    return df

def _update_db_maps(conn: psycopg2.extensions.connection, data: pd.DataFrame): 

    cur = conn.cursor()

    LOG.info ("Doing mapping checks")
    mapping_check = {
            "HomeCenter": "center_map",\
            "Computer Name": "device_map",\
            "software_hash": "software_map",\
            # "IP Address": "ip_map",\
            "OS": "os_map",\
                    }

    for coln, tbl in mapping_check.items():
        col = COL_REPLACE[coln]
        _checkmaps(conn, col, tbl, set(data[coln].unique()) )

    conn.commit()

if __name__ == '__main__':
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='XLSM requirements db loader')
    ap.add_argument('-n', '--dbname', type=str, default = 'apps')
    ap.add_argument('-f', '--csv_file', type=str, help='Name of the file which holds the software data')
    ap.add_argument('-c', '--chunk_size', type=int, default=100000, help='Chunk size (in rows) for loading iteration')

    # parse argv
    opts = ap.parse_args()

    if not opts.csv_file:
        LOG.fatal ("the --csv_file <file> parameter must be specified")
        ap.print_usage()
        exit()

    # load the data
    _insert_csv (opts.dbname, opts.csv_file, rowsize=opts.chunk_size)

    LOG.info ("finished")

