
import pandas as pd
from utility import *

# relational DB list from wikipedia
# list of db software to search for

RELATIONAL_DB_LIST_STR = """4th Dimension
Adabas D
Alpha Five
Apache Derby
Aster Data
Amazon Aurora
Altibase
CA Datacom
CA IDMS
Clarion
ClickHouse
Clustrix
CSQL
CUBRID
DataEase
Database Management Library
Dataphor
dBase
Derby aka Java DB
Empress Embedded Database
EXASolution
EnterpriseDB
eXtremeDB
FileMaker Pro
Firebird
FrontBase
Google Fusion Tables
Greenplum
GroveSite
H2
Helix database
HSQLDB
IBM DB2
IBM Lotus Approach
IBM DB2 Express-C
Infobright
Informix
Ingres
InterBase
InterSystems Caché
LibreOffice Base
Linter
MariaDB
MaxDB
MemSQL
Microsoft Access
Microsoft Jet Database Engine
Microsoft SQL Server
Microsoft SQL Server Express
SQL Azure
Cloud SQL Server
Microsoft Visual FoxPro
Mimer SQL
MonetDB
mSQL
MySQL
Netezza
NexusDB
NonStop SQL
NuoDB
Omnis Studio
Openbase
OpenLink Virtuoso
OpenLink Virtuoso Universal Server
OpenOffice.org Base
Oracle
Oracle Rdb for OpenVMS
Panorama
Pervasive PSQL
Polyhedra
PostgreSQL
Postgres Plus Advanced Server
Progress Software
RDM Embedded
RDM Server
R:Base
SAND CDBMS
SAP HANA
SAP Adaptive Server Enterprise
SAP IQ
Sybase IQ
SQL Anywhere
Sybase Adaptive Server Anywhere
Watcom SQL
solidDB
SQLBase
SQLite
SQream DB
SAP Advantage Database Server
Sybase Advantage Database Server
Teradata
Tibero
TimesTen
Trafodion
txtSQL
Unisys RDMS 2200
UniData
UniVerse
Vectorwise
Vertica
VoltDB"""

RELATIONAL_DB_LIST = RELATIONAL_DB_LIST_STR.split('\n')

# list of terms we DONT want to have in the name/path
DB_CONSTRAINT_TERMS = ['install', 'uninstall', 'ODBC', 'JDBC', 'updater', 'document', 'app']

def db_filter_constraint (db_software):
    db_constraint = f"""where install_location ilike '%{db_software}%'"""
    for term in DB_CONSTRAINT_TERMS:
        db_constraint += f""" and install_location not ilike '%{term}%'"""
    return db_constraint

#print (db_filter_constraint())

# Define a software search method or 2 for us
def db_software (software, conn):
    ''' Simple program to automate software searches '''
    
    return Query (conn, 'software_map', db_filter_constraint(software)).result
        
def db_software_histo (software, conn):
    '''Program to provide breakdown of types of software '''

    return HistoQuery(conn, 'software_map', 'software_name', db_filter_constraint(software)).result 
        
def db_software_count (conn, software, table='software_map', distinct=None):
    '''Determine count of matching software'''
    
    return CountQuery (conn, table, db_filter_constraint(software), distinct).result

def count_rdbs_types(conn):
    '''Count out all of the db software we can find'''
    
    data = []
    for db_software in RELATIONAL_DB_LIST:
        data.append(pd.DataFrame.from_dict({'software_name': db_software, 'num_types' : db_software_count(conn, db_software, distinct='software_name')['num']}))
    
    return pd.concat(data).sort_values(['num_types'], ascending=False)
    
def count_rdbs_installs(conn):
    
    data = []
    for db_software in RELATIONAL_DB_LIST:
        data.append(pd.DataFrame.from_dict({'software_name': db_software, 'num_installs' : db_software_count(conn, db_software, table='device_software')['num']}))
        
    return pd.concat(data).sort_values(['num_installs'], ascending=False)
