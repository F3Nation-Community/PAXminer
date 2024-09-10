#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the daily PAXminer backblast queries and data updates for all F3 regions using PAXminer.
'''

from slacker import Slacker
import pandas as pd
import os
import warnings

from db_connection_manager import DBConnectionManager

warnings.simplefilter(action='ignore', category=FutureWarning)

mydb1 = DBConnectionManager('../config/credentials.ini').connect()

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        sql = "SELECT * FROM paxminer.regions where region = 'Mobile'" # <-- Update this for whatever region is being tested
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions, columns={'region', 'slack_token', 'schema_name'})
finally:
    print('Getting list of regions that use PAXminer...')

for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    print('Executing user updates for region ' + region)
    os.system("./F3SlackUserLister.py " + db + " " + key)
    os.system("./F3SlackChannelLister.py " + db + " " + key)
    #os.system("./PAX_BD_Miner.py " + db + " " + key)
    print('----------------- End of Region Update -----------------\n')
print('\nPAXminer execution complete.')