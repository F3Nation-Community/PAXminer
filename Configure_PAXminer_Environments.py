#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script joins all required Slack channels and creates the log and plot directories for regions.
'''

from slacker import Slacker
import pandas as pd
import pymysql.cursors
import configparser
import os

# Set the working directory to the directory of the script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Configure AWS credentials
config = configparser.ConfigParser();
config.read('../config/credentials.ini');

# Configure AWS Credentials
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']
db = config['aws']['db']


#Define AWS Database connection criteria
mydb1 = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    db=db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

# Get list of regions and Slack tokens for PAXminer execution
try:
    with mydb1.cursor() as cursor:
        #sql = "SELECT * FROM paxminer.regions where active = 1 and firstf_channel IS NOT NULL" # <-- Update this for whatever region is being tested
        sql = "SELECT * FROM paxminer.regions where region = 'Lake_Wylie'"  # <-- Update this for whatever region is being tested
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