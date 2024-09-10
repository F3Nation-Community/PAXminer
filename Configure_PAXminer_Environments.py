#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script joins all required Slack channels and creates the log and plot directories for regions.
'''

from slacker import Slacker
import pandas as pd
import os

from db_connection_manager import DBConnectionManager

# Set the working directory to the directory of the script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

mydb1 = DBConnectionManager('../config/credentials.ini').connect()

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        sql = "SELECT * FROM paxminer.regions where active = 1 and firstf_channel IS NOT NULL" # <-- Update this for whatever region is being tested
        #sql = "SELECT * FROM paxminer.regions where region = 'Lake_Wylie'"  # <-- Update this for whatever region is being tested
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
    os.system("./Join_Channels_and_Create_Directories.py " + db + " " + key + " " + region + " " + firstf)
    print('----------------- End of Region Configuration -----------------\n')
print('\nPAXminer Configurations Complete.')