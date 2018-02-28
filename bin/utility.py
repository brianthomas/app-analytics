
import psycopg2
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit, leastsq

DEF_MAX_TIME = 104 # weeks for 2 years
MIN_TIME = -1  # weeks 

def fit_leastsq(p0, datax, datay, fitfunc, y_err=[]):
    ''' LeastSquares fit with no y errors
    Usage : 
    pfit, perr = fit_leastsq(pstart, xdata, ydata, ff)
    '''

    if len(y_err) == len(datay):
        # our error function when we have errors
        errfunc = lambda p, x, y, err: (y - fitfunc(p, x)) / err
    else:
        errfunc = lambda p, x, y, err: y - fitfunc(p, x) 

    pfit, pcov, infodict, errmsg, success = \
        leastsq(errfunc, p0, args=(datax, datay, y_err), full_output=1, epsfcn=0.0001)

    redchisq = -1.
    if (len(datay) > len(p0)) and pcov is not None:

        # calculate reduced chisq
        redchisq = redchisqg(datay, fitfunc(pfit, datax), nparam=len(p0), sd=y_err)

        # multiply pcov by redchisq 
        pcov = pcov * redchisq

    else:
        pcov = np.inf

    error = [] 
    for i in range(len(pfit)):
        try:
          error.append(np.absolute(pcov[i][i])**0.5)
        except:
          error.append( 0.00 )

    pfit_leastsq = pfit
    perr_leastsq = np.array(error) 

    return pfit_leastsq, perr_leastsq, redchisq

def fit_probability (pfit, redchisq):
    from scipy.stats import chisqprob
    deg = len(pfit)
    chisq = redchisq * deg
    return chisqprob(chisq, deg) 

def redchisqg(ydata, ymod, nparam=2, sd=[]):  
    """  
    Returns the reduced chi-square error statistic for an arbitrary model,   
    chisq/nu, where nu is the number of degrees of freedom. If individual   
    standard deviations (array sd) are supplied, then the chi-square error   
    statistic is computed as the sum of squared errors divided by the standard   
    deviations. See http://en.wikipedia.org/wiki/Goodness_of_fit for reference.  
   
    ydata,ymod,sd assumed to be Numpy arrays. n is an integer.  
   
    Usage:  
    >>> chisq=redchisqg(ydata,ymod,n,sd)  
    where  
       ydata : data  
       ymod : model evaluated at the same x points as ydata  
       n : number of free parameters in the model  
       sd : "standard deviation", e.g. uncertainties in ydata  

    """  
    # Chi-square statistic  
    if len(sd) > 0:  
        chisq=np.sum(((ydata-ymod)/sd)**2 ) 
    else:
        chisq=np.sum((ydata-ymod)**2) 
             
    # Number of degrees of freedom   
    deg=ydata.size-nparam
        
    return chisq/deg 

def create_connection (dbname):
    return psycopg2.connect(host="localhost",database=dbname, user="postgres", password="")

def create_vuln_title_map(dbname):
    """
    Returns a dictionary:
    key: group names
    values: set of indices of vulnerabilities that correspond
    """
    conn = create_connection(dbname)
    cur = conn.cursor()

    cur.execute('SELECT rowid, vulnerability_title FROM vuln_info;')
 
    vuln_titles = cur.fetchall()
    cur.close()
    conn.close()

    vuln_title_map = {'office': set(), 'server': set(),\
                 'outlook': set(), 'windows 7': set(),\
                 'sql server': set(), '.net': set(), 'windows xp': set()}

    for index, title in vuln_titles:
        for category in vuln_title_map:
            if category in title.lower():
                vuln_title_map[category].add(index)

    return vuln_title_map

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


def load_data_from_db(conn, table, max_time=730, use_week=False):

    # load our data from the database and prep it
    
    if use_week:

        data_obj = WeekQuery(table, conn)
        x = np.array(data_obj.df.weeks_since_release, dtype='float64')
   
    else:

        data_obj = DayQuery(table, conn)
        x = np.array(data_obj.df.days_since_release, dtype='float64')

    y = np.array(data_obj.df.avg * 100., dtype='float64')
    y_err = np.array(data_obj.df.err * 100. , dtype='float64')
    
    # Data prep
    # just use the data from the indicated time period 0 -> max_time
    max_index = len(x)-1 
    while (x[max_index] > max_time):
        max_index = max_index - 1
    x = x[0:max_index]
    y = y[0:max_index]
    y_err = y_err[0:max_index]

    # Error correction. In the db, we estimated as standard deviation about the mean, however
    # this is not quite right for discrete variables (we only have 0 or 1 values). 
    # https://nzmaths.co.nz/category/glossary/standard-deviation-discrete-random-variable
    # shows us that we need to multiple by square of the probability
 
    # This is fraught with peril however as we dont know the underlying probability distribution.
    # Possibly we can get away with just estimating equal chances # of 0 or a 1 result for "is_patched". 
    # IF this is reasonable (not sure), then in all cases, each value has squareroot of .5 
    # or approx .707
    # y_err = y_err * 0.707
    
    return (x, y, y_err)

def load_data_from_db_wk (conn, table, max_time=DEF_MAX_TIME):
    return load_data_from_db(conn, table, max_time, use_week=True)

# calculate the midpoint in the brokenline function
def midpoint(p, perr):
    return (50. / p[0], ((50./(p[0] - perr[0])) - (50./(p[0] + perr[0])))/2.)


class DayQuery:

    @staticmethod
    def _do_query (tablename, conn, constraint):
        query_str = f"""SELECT * FROM {tablename} {constraint} order by days_since_release;"""
        return pd.read_sql_query(query_str, conn)

    def __init__(self, tablename, conn, constraint=""):
        self.df = DayQuery._do_query(tablename, conn, constraint)

class WeekQuery():

    @staticmethod
    def _do_query (tablename, conn):
        query_str = f"""SELECT * FROM {tablename} order by weeks_since_release;"""
        return pd.read_sql_query(query_str, conn)

    def __init__(self, tablename, conn):
        self.df = WeekQuery._do_query(tablename, conn)


