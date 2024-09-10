#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the monthly PAXcharter backblast queries and data updates for all F3 regions using PAXminer.
'''

from slacker import Slacker
import pandas as pd
import os

from db_connection_manager import DBConnectionManager

mydb1 = DBConnectionManager('../config/credentials.ini').connect()

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        #sql = "SELECT * FROM paxminer.regions where send_region_stats = 1" # <-- Update this for whatever region is being tested
        sql = "SELECT * FROM paxminer.regions where region = 'Geneva'"  # <-- Update this for whatever region is being tested
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
    #os.system("./PAXcharter.py " + db + " " + key)
    #os.system("./UniquePAXCharter.py " + db + " " + key + " " + region + " " + firstf)
    #os.system("./QCharter.py " + db + " " + key + " " + region + " " + firstf)
    #os.system("./Leaderboard_Charter.py " + db + " " + key + " " + region + " " + firstf)
    #os.system("./LeaderboardByAO_Charter.py " + db + " " + key + " " + region + " " + firstf)
    #os.system("./Join_Channels_and_Create_Directories.py " + db + " " + key + " " + region + " " + firstf)
    os.system("./AOCharter.py " + db + " " + key + " " + region + " " + firstf)
    print('----------------- End of Region Update -----------------\n')
print('\nPAXcharter execution complete.')