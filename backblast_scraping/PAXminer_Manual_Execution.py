#!/usr/bin/env python3
import pandas as pd
import pymysql.cursors
import configparser
import os
import warnings
from PAX_BD_Miner import run_pax_bd_miner
warnings.simplefilter(action='ignore', category=FutureWarning)

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
        sql = "SELECT * FROM paxminer.regions where schema_name = 'f3stlcity'"
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

    run_pax_bd_miner(host, port, user, password, db, key)
    print('----------------- End of Region Update -----------------\n')
mydb1.close()
print('\nPAXminer execution complete.')