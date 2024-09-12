#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the daily PAXminer backblast queries and data updates for all F3 regions using PAXminer.
'''

from slacker import Slacker
import pandas as pd
import pymysql.cursors
import configparser
import os
import sys

# Set the working directory to the directory of the script
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Set RegEx range for which regions will be queried. Command line input parameter 1 should be a regex range (e.g. A-M) which will search for all regions starting with A through M.
region_regex = sys.argv[1]

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
        sql = "SELECT * from paxminer.regions WHERE active = 1 AND region REGEXP '^[" + region_regex + "]'"
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions, columns=['region', 'slack_token', 'schema_name'])
finally:
    print('Getting list of regions that use PAXminer...')

for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    print('Executing user updates for region ' + region)
    #os.system("./F3SlackUserLister.py " + db + " " + key)
    #os.system("./F3SlackChannelLister.py " + db + " " + key)
    #os.system("./BDminer.py " + db + " " + key)
    #os.system("./PAXminer.py " + db + " " + key)
    print('----------------- End of Region Update -----------------\n')