#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the monthly PAXcharter backblast queries and data updates for all F3 regions using PAXminer.
'''

from slacker import Slacker
import pandas as pd
import os
import sys

from db_connection_manager import DBConnectionManager

# Set the working directory to the directory of the script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

mydb1 = DBConnectionManager('../config/credentials.ini').connect()

# Set RegEx range for which regions will be queried. Command line input parameter 1 should be a regex range (e.g. A-M) which will search for all regions starting with A through M.
region_regex = sys.argv[1]

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        sql = "SELECT * FROM paxminer.regions where firstf_channel IS NOT NULL AND send_pax_charts = 1 AND region REGEXP '^[" + region_regex + "]'""
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions)
finally:
    print('Getting list of regions that use PAXminer...')

for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    firstf = row['firstf_channel']
    #firstf = 'U0187M4NWG4' # <--- Use this if sending a test msg to a specific user
    print('Processing statistics for region ' + region)
    os.system("./PAXcharter.py " + db + " " + key)
    print('----------------- End of Region Update -----------------\n')
print('\nPAXcharter execution complete.')