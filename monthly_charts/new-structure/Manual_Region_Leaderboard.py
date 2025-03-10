from region_leaderboard_helper import region_leaderboard_run

import pymysql.cursors
import configparser
import sys
from slack_sdk import WebClient

# Configure AWS credentials
config = configparser.ConfigParser();
config.read('../../config/credentials.ini');
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']

db = sys.argv[1]
key = sys.argv[2]
firstf = sys.argv[3]
slack = WebClient(token=key)
region_name = sys.argv[4]

def init_db(host, port, user, password, region_db):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=region_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
        )

region_leaderboard_run(region_name, init_db(host, port, user, password, db), key, firstf, db)