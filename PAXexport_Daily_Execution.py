#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script executes the daily PAXminer export to tab delimited files for all F3 regions using PAXminer.
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
db = 'paxminer'


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
        sql = "SELECT * FROM paxminer.regions"
        cursor.execute(sql)
        regions = cursor.fetchall()
        regions_df = pd.DataFrame(regions, columns={'region', 'slack_token', 'schema_name'})
finally:
    print('Getting list of regions for export...')

for index, row in regions_df.iterrows():
    region = row['region']
    key = row['slack_token']
    db = row['schema_name']
    print('Exporting data for region ' + region)
    os.system("./DelimFileWriter.py " + db + " " + key)
    print('----------------- End of Region Export -----------------\n')