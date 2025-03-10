from ao_leaderboard_helper import region_run

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
#db = config['aws']['db']
db = sys.argv[1]

# Set Slack token
key = sys.argv[2]
slack = WebClient(token=key)

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

region_run(init_db(host, port, user, password, db), db, key)