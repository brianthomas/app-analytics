import selenium
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import logging

LOG = logging.getLogger()

BASE_URL = "https://asus.nasa.gov/r/edw/security-profile/key/"
PAGE_LOAD_WAIT_TIME = 1

def parse (base_url, start_ssp, end_ssp):

    data = {}

    LOG.debug ("getting browser")
    browser=webdriver.Chrome()

    # loop thru pages and parse as needed
    for page_number in range(start_ssp,end_ssp+1):

        url = BASE_URL + str(page_number)

        data [url] = {}

        LOG.debug("getting webpage: "+url)
        browser.get(url)
        time.sleep(PAGE_LOAD_WAIT_TIME)

        # IF we hit the Launchpad login deal with that
        try:
            lp_button_select = browser.find_element_by_id("SCLOGIN")
            if lp_button_select:
                lp_button_select.click()
                time.sleep(PAGE_LOAD_WAIT_TIME*2)
        except selenium.common.exceptions.NoSuchElementException:
            pass

        # grab page as parsable soup
        soup=BeautifulSoup(browser.page_source, "lxml")

        # now parse out the table in the page with info we want
        tables = soup.find_all('table')
        for table in tables:
            tid = table.get('id')

            # check for the right table which has our SSP values
            if tid != None and tid == "tbl_tc_d_c":

                # parse out SSP metadata and store in mem
                for row in table.find_all('tr'):
                    key = None
                    value = None
                    for item in row.find_all('td'):
                        iclass = item['class'][0]
                        if iclass == "key":
                            key = item.string
                        else:
                            value = item.string
          
                    # can this happen? not sure need this check
                    if key != None and value != None:
                        data[url][key] = value

                break

    # return SSP metadata 
    return data


if __name__ == '__main__':

    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='SSP metadata scrapper from EDW-Bigfix pages')
    ap.add_argument('-s', '--start_ssp', type=int, default=1, help='Starting index of SSP we want')
    ap.add_argument('-e', '--end_ssp', type=int, default=10000, help='Ending index of SSP we want')

    # parse argv
    opts = ap.parse_args()

    data = parse(BASE_URL, opts.start_ssp, opts.end_ssp)
    print (data)


