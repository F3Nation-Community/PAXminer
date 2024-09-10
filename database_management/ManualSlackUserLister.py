from F3SlackUserLister import database_slack_user_update

import sys
from slack_sdk import WebClient

from db_connection_manager import DBConnectionManager

db = sys.argv[1]
connection = DBConnectionManager('../config/credentials.ini').connect(db)

# Set Slack token
key = sys.argv[2]
slack = WebClient(token=key)

database_slack_user_update(db, key, False, connection)
