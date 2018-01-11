'''
Merge scraped SSP Json files into single csv file
'''

import json

def _parse(files: str) -> dict:

    merged_data = [] 

    for file in files:
        with open(file, encoding='utf-8') as json_file:
            text = json_file.read()
            text = text.replace("\'", "\"")
            ddict = json.loads(text)

            for item in [ ddict[key] for key in ddict.keys() ]: 
                merged_data.append(item)
         
    return merged_data

if __name__ == '__main__':
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(description='merge ssp json to csv util')
    ap.add_argument('-f', '--files', nargs='+', type=str, help='Name of the SSP json files to merge')

    # parse argv
    opts = ap.parse_args()

    if not opts.files:
        print ("the --files <file> parameter must be specified")
        ap.print_usage()
        exit()

    # parse out out data into single csv representation
    merged_data = _parse(opts.files)

    #print ("got # merged data:"+ str(len(merged_data)))

    # print back to STDOUT
    for item in merged_data:
        print(str(item))

