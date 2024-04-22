#!/usr/bin/env python3
'''
This script was written by Beaker from F3STL. Questions? @srschaecher on twitter or srschaecher@gmail.com.
This script queries Slack for User, Channel, and Conversation (channel) history and then parses all conversations to find Backblasts.
All Backblasts are then parsed to collect the BEATDOWN information for any given workout and puts those attendance records into the AWS F3STL database for recordkeeping.
'''

import warnings
from slack_sdk import WebClient
from datetime import datetime, timedelta
import time
import dateparser
import pandas as pd
import pytz
import re
import pymysql.cursors
import configparser
import sys
import logging
import math
import warnings
from BD_Update_Utils import determine_db_action, find_match, retrievePreviousBackblasts, DbAction

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

MIN_BACKBLAST = 'Backblast:AO:PAX:@x@yQ:@xCount:0'
SECONDS_PER_DAY = 86400
LOOKBACK_DAYS = 7
LOOKBACK_SECONDS = SECONDS_PER_DAY * LOOKBACK_DAYS

pd.options.mode.chained_assignment = None  # default='warn'

# Configure AWS credentials
config = configparser.ConfigParser();
config.read('../config/credentials.ini');
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']
db = sys.argv[1] # Use this for the multi-region automated update

# Set Slack token
key = sys.argv[2] # Use this for the multi-region automated update
slack = WebClient(token=key)

# Enable rate limited error retries
rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=5)
slack.retry_handlers.append(rate_limit_handler)

#Define AWS Database connection criteria
mydb = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    db=db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

# Set epoch and yesterday's timestamp for datetime calculations
epoch = datetime(1970, 1, 1)
yesterday = datetime.now() - timedelta(days = 1)
today = datetime.now()
cutoff_date = today - timedelta(days = 7) # This tells BDminer to go back up to N days for message age
current_ts = time.time()
cutoff_ts = current_ts - LOOKBACK_SECONDS
cutoff_date = cutoff_date.strftime('%Y-%m-%d')
date_time = today.strftime("%m/%d/%Y, %H:%M:%S")

# Set up logging
logging.basicConfig(filename='../logs/BD_PAX_miner.log',
                            filemode = 'a',
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
logging.info("Running combined BD+PAXminer for " + db)
pm_log_text = date_time + " CDT: Executing hourly PAXminer run for " + db + "\n"

# Make users Data Frame
column_names = ['user_id', 'user_name', 'real_name']
users_df = pd.DataFrame(columns = column_names)
users_df.loc[len(users_df.index)] = ['APP', 'BackblastApp', 'BackblastApp']
data = ''
while True:
    users_response = slack.users_list(limit=1000, cursor=data)
    response_metadata = users_response.get('response_metadata', {})
    next_cursor = response_metadata.get('next_cursor')
    users = users_response.data['members']
    users_df_tmp = pd.json_normalize(users)
    users_df_tmp = users_df_tmp[['id', 'profile.display_name', 'profile.real_name']]
    users_df_tmp = users_df_tmp.rename(columns={'id' : 'user_id', 'profile.display_name' : 'user_name', 'profile.real_name' : 'real_name'})
    users_df = users_df.append(users_df_tmp, ignore_index=True)
    if next_cursor:
        # Keep going from next offset.
        #print('next_cursor =' + next_cursor)
        data = next_cursor
    else:
        break
for index, row in users_df.iterrows():
    un_tmp = row['user_name']
    rn_tmp = row['real_name']
    if un_tmp == "" :
        row['user_name'] = rn_tmp

# Retrieve Channel List from AWS database (backblast = 1 denotes which channels to search for backblasts)
try:
    with mydb.cursor() as cursor:
        sql = "SELECT channel_id, ao FROM aos WHERE backblast = 1 AND archived = 0"
        cursor.execute(sql)
        channels = cursor.fetchall()
        channels_df = pd.DataFrame(channels, columns={'channel_id', 'ao'})
finally:
    print('Pulling current beatdown records...')

# Retrieve backblast entries from regional database for comparison to identify new or updated posts
try:
    previously_saved_beatdowns = retrievePreviousBackblasts(mydb, cutoff_ts)
finally:
    print('Looking for new backblasts from Slack...')

# Get all channel conversation
messages_df = pd.DataFrame([]) #creates an empty dataframe to append to
for id in channels_df['channel_id']:
    data = ''
    pages = 1
    while True:
        try:
            #print("Checking channel " + id) # <-- Use this if debugging any slack channels throwing errors
            response = slack.conversations_history(channel=id, cursor=data)
            response_metadata = response.get('response_metadata', {})
            try:
                next_cursor = response_metadata.get('next_cursor')
            except:
                pass
            messages = response.data['messages']
            temp_df = pd.json_normalize(messages)
            try:
                temp_df = temp_df[['user', 'type', 'text', 'ts', 'edited.ts']]
            except:
                temp_df = temp_df[['user', 'type', 'text', 'ts']]
                temp_df['edited.ts'] = "NA"
            finally:
                temp_df["user"]=temp_df["user"].fillna("APP")
                temp_df = temp_df.rename(columns={'user' : 'user_id', 'type' : 'message_type', 'ts' : 'timestamp', 'edited.ts' : 'ts_edited'})
                temp_df["channel_id"] = id
                messages_df = messages_df.append(temp_df, ignore_index=True)
        except:
            print("Error: Unable to access Slack channel:", id, "in region:",db)
            logging.warning("Error: Unable to access Slack channel %s in region %s", id, db)
            pm_log_text += "Error: Unable to access Slack channel " + id + " in region " + db + "\n"
        if next_cursor != "None":
            # Keep going from next offset.
            data = next_cursor
            if pages == 1: ## Total number of pages to query from Slack
                break
            pages = pages + 1
        else:
            break

# Calculate Date and Time columns
msg_date = []
msg_time = []
for ts in messages_df['timestamp']:
        seconds_since_epoch = float(ts)
        dt = epoch + timedelta(seconds=seconds_since_epoch)
        dt = dt.replace(tzinfo=pytz.utc)
        dt = dt.astimezone(pytz.timezone('America/Chicago'))
        msg_date.append(dt.strftime('%Y-%m-%d'))
        msg_time.append(dt.strftime('%H:%M:%S'))
messages_df['msg_date'] = msg_date
messages_df['time'] = msg_time


# Merge the data frames into 1 joined DF
f3_df = pd.merge(messages_df, users_df)
f3_df = pd.merge(f3_df,channels_df)
f3_df = f3_df[['timestamp', 'ts_edited', 'msg_date', 'time', 'channel_id', 'ao', 'user_id', 'user_name', 'real_name', 'text']]
f3_df['ts_edited'] = f3_df['ts_edited'].fillna('NA')

# Now find only backblast messages (either "Backblast" or "Back Blast") - note .casefold() denotes case insensitivity - and pull out the PAX user ID's identified within
# This pattern finds username links followed by commas: pat = r'(?<=\\xa0).+?(?=,)'
pat = r'(?<=\<).+?(?=>)' # This pattern finds username links within brackets <>
bd_df = pd.DataFrame([])
pax_attendance_df = pd.DataFrame([])
warnings.filterwarnings("ignore", category=DeprecationWarning) #This prevents displaying the Deprecation Warning that is present for the RegEx lookahead function used below

def bd_info():
    # Find the Q information
    qline = re.findall(r'(?<=\n)\*?V?Qs?\*?:.+?(?=\n)', str(text_tmp), re.MULTILINE) #This is regex looking for \nQ: with or without an * before Q
    qids = re.findall(pat, str(qline), re.MULTILINE)
    qids = [re.sub(r'@', '', i) for i in qids]
    if qids:
        qid = qids[0]
    else:
        qid = 'NA'
    if len(qids) > 1:
        coqid = qids[1]
    else:
        coqid = 'NA'
    # Find the PAX Count line (if the Q put one in the BB)
    pax_count = re.search(r'(?<=\n)\*?(?i)Count\*?:\*?.+?(?:$|\n)', str(text_tmp), re.IGNORECASE)
    if pax_count:
        pass
    else:
        pax_count = re.search(r'(?<=\n)\*?(?i)Total\*?:\*?.+?(?=\n)', str(text_tmp), re.IGNORECASE)
    if pax_count:
        pax_count = pax_count.group()
        pax_count = re.findall('\d+', str(pax_count))
        if pax_count:
            pax_count = int(pax_count[0])
        else:
            pax_count = -1
    if isinstance(pax_count, int):
        pass
    else:
        pax_count = -1
    # Find the FNGs line
    fngline = re.findall(r'(?<=\n)\*?FNGs\*?:\*?.+?(?=\n)', str(text_tmp), re.MULTILINE)  # This is regex looking for \nFNGs: with or without an * before Q
    if fngline:
        fngline = fngline[0]
        fngs = re.sub('\*?FNGs\*?:\s?', '', str(fngline))
        fngs = fngs.strip()
    else:
        fngs = 'None listed'
    #Find the Date:
    dateline = re.findall(r'(?<=\n)Date:.+?(?=\n)', str(text_tmp), re.IGNORECASE)
    if dateline:
        dateline = re.sub('xa0', ' ', str(dateline), flags=re.I)
        dateline = re.sub("Date:\s?", '', str(dateline), flags=re.I)
        dateline = dateparser.parse(dateline) #dateparser is a flexible date module that can understand many different date formats
        if dateline is None:
            date_tmp = '2099-12-31' #sets a date many years in the future just to catch this error later (needs to be a future date)
        else:
            date_tmp = str(datetime.strftime(dateline, '%Y-%m-%d'))
    else:
        date_tmp = msg_date
    #Find the AO line
    aoline = re.findall(r'(?<=\n)\*?AO\*?:\*?.+?(?=\n)', str(text_tmp),re.MULTILINE)  # This is regex looking for \nAO: with or without an *
    if aoline:
        ao_name = re.sub('\*?AO\*?:\s?', '', str(aoline))
        ao_name = ao_name.strip()
    else:
        ao_name = 'Unknown'
    global bd_df
    new_row = {'timestamp' : timestamp, 'ts_edited' : ts_edited, 'msg_date' : msg_date, 'ao_id' : ao_tmp, 'bd_date' : date_tmp, 'q_user_id' : qid, 'coq_user_id' : coqid, 'pax_count' : pax_count, 'backblast' : text_tmp, 'fngs' : fngs, 'user_name' : user_name, 'user_id' : user_id, 'ao_name' : ao_name}
    return new_row

# Looking within a backblast, retrieves a list of pax
# Adds the Q lines to the pax list as well.
# Deduplicates the list to account for PAX listed multiple lines, or a Q who is also listed in the PAX list.
def list_pax(beatdown_text):
    #paxline = [line for line in beatdown_text.split('\n') if 'pax'.casefold() in line.casefold()]
    paxline = re.findall(r'(?<=\n)\*?(?i)PAX\*?:\*?.+?(?=\n)', str(beatdown_text), re.MULTILINE) #This is a case insensitive regex looking for \nPAX with or without an * before PAX
    
    pax = re.findall(pat, str(paxline), re.MULTILINE)
    pax = [re.sub(r'@','', i) for i in pax]

    # Add q and co-q as attendees
    qline = re.findall(r'(?<=\n)\*?V?Qs?\*?:.+?(?=\n)', str(beatdown_text), re.MULTILINE) #This is regex looking for \nQ: with or without an * before Q
    qids = re.findall(pat, str(qline), re.MULTILINE)
    qids = [re.sub(r'@', '', i) for i in qids]
    if qids:
        pax.extend(qids)

    # Remove duplicates
    return list(set(pax))

# Checks if the message qualifies as a backblast.
# Must meet the required length of a minimum backblast.
# Must contain the backblast keyword.
# Must Contain one other backblast collection field.
def isBackblastMessage(potential_backblast):
    return ( len(str(text_tmp)) > len(MIN_BACKBLAST)) and containsBackblastKeyword(potential_backblast=potential_backblast) and (
        ("PAX:" in potential_backblast) or ("Q:" in potential_backblast) or ("AO:" in potential_backblast) or ("DATE:" in potential_backblast)
    )


# Searches the potential backblast and returns a truthy value if it matches one of the included regexes.
def containsBackblastKeyword(potential_backblast): 
    return (
        re.findall('^Slackblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Backblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Backblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Back blast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Back Blast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Slack blast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Sackblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Slackblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Slack blast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Sackblast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Slackbast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Slackbast', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Sackdraft', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^\*Sackdraft', potential_backblast, re.IGNORECASE | re.MULTILINE) or 
        re.findall('^Back Blast', potential_backblast, re.IGNORECASE | re.MULTILINE)
    )

def isValidDate(date):
    lookback_valid_date = (today - timedelta(days = 30 )).strftime('%Y-%m-%d')
    forward_valid_date = (today + timedelta(days = 2)).strftime('%Y-%m-%d')

    if date == '2099-12-31':
        return False
    elif date < lookback_valid_date:
        return False
    elif date > forward_valid_date:
        return False
    else :
        return True

# Taking a dataframe of beatdown attendance, inserts these records into the database.
# If the database action is 'UPDATE', we clear out all the records that were associated with this beatdown previous to inserting.
# This prevents extra entries, in the scenario where 1) PAX that were mistakenly added, or 2) if one of the compound keys changed ( such as ao or q )
def insert_beatdown_attendance(dbConn, cursor, beatdown_attendance, database_action, beatdownkey):
    inserts = 0
    try:
        if database_action == DbAction.UPDATE:
            if beatdownkey:
                deleteSQL = "DELETE FROM bd_attendance WHERE timestamp = %s and timestamp is not NULL"
                cursor.execute(deleteSQL, beatdownkey)
                dbConn.commit()

        for index, row in beatdown_attendance.iterrows():
            sql11 = "INSERT INTO bd_attendance (timestamp, ts_edited, user_id, ao_id, date, q_user_id) VALUES (%s, %s, %s, %s, %s, %s)"
            timestamp = row['timestamp']
            ts_edited = row['ts_edited']
            user_id_tmp = row['user_id']
            msg_date = row['msg_date']
            ao_tmp = row['ao']
            date_tmp = row['bd_date']
            q_user_id = row['q_user_id']
            coq_user_id = row['coq_user_id']

            # For the user who is also the CO-Q, list their record in the database as them as the Q.
            if user_id_tmp == coq_user_id:
                val = (timestamp, ts_edited, user_id_tmp, ao_tmp, date_tmp, coq_user_id)
            else :
                val = (timestamp, ts_edited, user_id_tmp, ao_tmp, date_tmp, q_user_id)
            
            if msg_date > cutoff_date:
                if date_tmp == '2099-12-31':
                    print('Backblast error on Date - AO:', ao_tmp, 'Date:', date_tmp, 'Posted By:', user_id_tmp)
                else:
                    if q_user_id != 'NA':
                        try :
                            cursor.execute(sql11, val)
                            if cursor.rowcount > 0:
                                print(cursor.rowcount, "record inserted for", user_id_tmp, "at", ao_tmp, "on", date_tmp, "with Q =", q_user_id)
                                inserts = inserts + 1
                        except Exception as error:
                            print("An error occurred writing to the datastore", error)
                            logging.error("An error occured writing to the datastore: %s", error)
    except Exception as error:
        print("An error occurred inserting_beatdown_attendance", error)
        logging.error("An error occured inserting_beatdown_attendance: %s", error)                        
    finally:
        return inserts

def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default
    
# Iterate through the new bd_df dataframe, pull out the channel_name, date, and text line from Slack. Process the text line to find the beatdown info
for index, row in f3_df.iterrows():
    ao_tmp = row['channel_id']
    timestamp = row['timestamp']
    ts_edited = row['ts_edited']
    msg_date = row['msg_date']
    text_tmp = row['text']
    text_tmp = re.sub('_\\xa0', ' ', str(text_tmp))
    text_tmp = re.sub('\\xa0', ' ', str(text_tmp))
    text_tmp = re.sub('_\*', '', str(text_tmp))
    text_tmp = re.sub('\*_', '', str(text_tmp))
    text_tmp = re.sub('\*', '', str(text_tmp))
    user_name = row['user_name']
    user_id = row['user_id']
    if len(str(text_tmp)) <= len(MIN_BACKBLAST):
        continue
    
    if containsBackblastKeyword(text_tmp):
        new_row = bd_info()
        if float(new_row["timestamp"]) > float(cutoff_ts):
            upsertAction = determine_db_action(new_row, find_match(new_row, previously_saved_beatdowns))

            new_row["database_action"] = upsertAction
            
            bd_df = bd_df.append(new_row, ignore_index = True)

# Now connect to the AWS database and insert some rows!
try:
    with mydb.cursor() as cursor:
        for index, row in bd_df.iterrows():
            qc = 1
            send_q_msg = 0
            
            update_sql = """
                UPDATE beatdowns 
                SET timestamp=%s, ts_edited=%s, ao_id=%s, bd_date=%s, q_user_id=%s, coq_user_id=%s, pax_count=%s, backblast=%s, fngs=%s
                WHERE timestamp=%s
                LIMIT 1
            """
            insert_sql = "INSERT into beatdowns (timestamp, ts_edited, ao_id, bd_date, q_user_id, coq_user_id, pax_count, backblast, fngs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            timestamp = row['timestamp']
            ts_edited = row['ts_edited']
            ao_id = row['ao_id']
            msg_date = row['msg_date']
            bd_date = row['bd_date']
            q_user_id = row['q_user_id']
            coq_user_id = row['coq_user_id']
            pax_count = row['pax_count']
            backblast = row['backblast']
            user_name = row['user_name']
            user_id = row['user_id']
            fngs = row['fngs']
            msg_link = slack.chat_getPermalink(channel=ao_id, message_ts=timestamp)["permalink"]
            ao_name = row['ao_name']
            database_action = row["database_action"]

            val = (timestamp, ts_edited, ao_id, bd_date, q_user_id, coq_user_id, pax_count, backblast, fngs)
            # for Slackblast users, set the user_id as the Q
            appnames = ['slackblast', 'Slackblast']
            if user_name in appnames:
                user_id = q_user_id
                user_name = 'Q'
            q_error_text = "Hey " + user_name + " - I see a backblast you posted on " + msg_date + " at <#" + ao_id + "> (<" + msg_link + "|link>). Here's what happened when I tried to process it: \n"
            # pm_log_text += "- Processing <" + msg_link + "|this> backblast."

            if database_action == DbAction.IGNORE:
                logging.debug("Encountered a bblast already recorded and has not been modified. Skipping import")
                continue
            
            if q_user_id == 'NA':
                logging.warning("Q error for AO: %s, Date: %s, backblast from Q %s (ID %s) not imported", ao_id, msg_date, user_name, user_id)
                print('Backblast error on Q at AO:', ao_id, 'Date:', msg_date, 'Posted By:', user_name, ". Slack message sent to Q. bd: ", bd_date, "cutoff:", cutoff_date)
                pm_log_text +=  " - Backblast error on Q at AO: <#" + ao_id + "> Date: " + msg_date + " Posted By: " + user_name + ". Slack message sent to Q.\n"
                if user_id != 'APP':
                    q_error_text += " - ERROR: The Q is not present or not tagged correctly. Please ensure the Q is tagged using @PAX_NAME \n"
                    send_q_msg = 2
                qc = 0
            else:
                pass
            if pax_count == -1:
                logging.warning("Count error for AO: %s, Date: %s, backblast from Q %s (ID %s) not imported", ao_id, msg_date, user_name, user_id)
                print('Backblast error on Count - AO:', ao_id, 'Date:', msg_date, 'Posted By:', user_name, ". Slack message sent to Q.")
                pm_log_text += " - Backblast error on Count at AO: <#" + ao_id + "> Date: " + msg_date + " Posted By: " + user_name + ". Slack message sent to Q.\n"
                if user_id != 'APP':
                    q_error_text += " - ERROR: The Count is not present or not entered correctly. The correct syntax is Count: XX - Use digits please. \n"
                    send_q_msg = 2
                qc = 0
            else:
                pass
        
            if not isValidDate(bd_date):
                logging.warning("Date error for AO: %s, Date: %s, backblast from Q %s (ID %s) not imported", ao_id, msg_date, user_name, user_id)
                print('Backblast error on Date - AO:', ao_id, 'Date:', msg_date, 'Posted By:', user_name,". Slack message sent to Q. bd: ", bd_date, "cutoff:", cutoff_date)
                pm_log_text += " - Backblast error on Date - AO: <#" + ao_id + "> Date: " + msg_date + " Posted By: " + user_name + ". Slack message sent to Q.\n"
                if user_id != 'APP':
                    q_error_text += " - ERROR: The Date is not entered correctly. I can understand most common date formats like Date: 12-25-2020, Date: 2021-12-25, Date: 12/25/21, or Date: December 25, 2021. Common mistakes include a date from the future or a date with the time appended.\n"
                    send_q_msg = 2
                qc = 0
            
            if qc == 1:
                try :
                    if database_action == DbAction.UPDATE :
                        cursor.execute(update_sql, val + (timestamp,))
                    else:
                        cursor.execute(insert_sql, val)
                    
                    mydb.commit()
                except Exception as error:
                    print("An error occurred writing to the datastore", error)
                    logging.error("An error occured writing to the datastore: %s", error)

                if cursor.rowcount == 1:
                    print(cursor.rowcount, "records inserted.")
                    print('Beatdown Date:', bd_date)
                    print('Message Posting Date:', msg_date)
                    print('AO:', ao_name)
                    print('Q:', q_user_id)
                    print('Co-Q', coq_user_id)
                    print('Pax Count:',pax_count)
                    print('fngs:', fngs)
                    if database_action == DbAction.UPDATE :
                        pm_log_text += " - Backblast successfully updated for AO: <#" + ao_id + "> Date: " + msg_date + " Posted By: " + user_name + "\n"
                        if user_id != 'APP':
                            q_error_text += " - Successfully updated your backblast after it had been changed for " + bd_date + " at <#" + ao_id + ">. I see you had " + str(math.trunc(pax_count)) + " PAX in attendance and FNGs were: " + str(fngs) + ". Thanks for posting and updating your BB! \n"
                            send_q_msg = 1
                    else:
                        pm_log_text += " - Backblast successfully imported for AO: <#" + ao_id + "> Date: " + msg_date + " Posted By: " + user_name + "\n"
                        if user_id != 'APP':
                            q_error_text += " - Successfully imported your backblast for " + bd_date + " at <#" + ao_id + ">. I see you had " + str(math.trunc(pax_count)) + " PAX in attendance and FNGs were: " + str(fngs) + ". Thanks for posting your BB! \n"
                            send_q_msg = 1
                    
                    print("Slack message sent to Q.")
                    logging.info("Backblast imported for AO: %s, Date: %s", ao_id, bd_date)

                    pax = list_pax(backblast)

                    if pax:
                        logging.info("Inserting PAX attendance for AO: %s, Date: %s", ao_id, bd_date)
                        pax_df = pd.DataFrame(pax)
                        pax_df['timestamp'] = timestamp
                        pax_df['ts_edited'] = ts_edited
                        pax_df.columns =['user_id', 'timestamp', 'ts_edited']

                        pax_df['ao'] = ao_id
                        pax_df['bd_date'] = bd_date
                        pax_df['msg_date'] = msg_date
                        pax_df['q_user_id'] = q_user_id
                        pax_df["coq_user_id"] = coq_user_id
                        inserts = insert_beatdown_attendance(mydb, cursor, pax_df, database_action, timestamp)
                        mydb.commit()

                        logging.info("PAX attendance updates complete: Inserted %s new PAX attendance records for AO: %s, Date: %s", inserts, ao_id, bd_date)
                    else:
                        logging.info("No PAX Found in Attendance for AO: %s, Date: %s", ao_id, bd_date)
            else:
                pass
            
            if send_q_msg == 2:
                q_error_text += "You can also check for other common mistakes that cause errors - such as spaces at the beginning of Date:, Q:, AO:, or other lines, or even other messages you may have posted that begin with the word Backblast."

                # Only send the message at 3pm each day or if the backblast was posted in the last hour
                try:
                    if today.hour == 15 or  ( safe_cast(ts_edited, float) or safe_cast(timestamp, float) >= ( current_ts - 3600)):
                        send_q_msg = 2
                    else:
                        send_q_msg = 0
                except Exception as error:
                    print("An error occurred determining to send an error message", error)
                    logging.error("An error occured determining to send an error message: %s", error)
                    
            if send_q_msg > 0:
                slack.chat_postMessage(channel=user_id, text=q_error_text)

        sql3 = "UPDATE beatdowns SET coq_user_id=NULL where coq_user_id = 'NA'"
        cursor.execute(sql3)
        mydb.commit()

        sql4 = "UPDATE beatdowns SET fng_count=0 where fngs in ('none', 'None', 'None listed', 'NA', 'zero', '-', '') AND fng_count IS NULL"
        cursor.execute(sql4)
        mydb.commit()

        sql5 = "UPDATE beatdowns SET fng_count = 0 where fngs like '0%' AND fng_count IS NULL"
        cursor.execute(sql5)
        mydb.commit()

        sql6 = "UPDATE beatdowns SET fng_count = 1 where fngs like '1%' AND fng_count IS NULL"
        cursor.execute(sql6)
        mydb.commit()

        sql7 = "UPDATE beatdowns SET fng_count = 2 where fngs like '2%' AND fng_count IS NULL"
        cursor.execute(sql7)
        mydb.commit()

        sql8 = "UPDATE beatdowns SET fng_count = 3 where fngs like '3%' AND fng_count IS NULL"
        cursor.execute(sql8)
        mydb.commit()

        sql9 = "UPDATE beatdowns SET fng_count = 4 where fngs like '4%' AND fng_count IS NULL"
        cursor.execute(sql9)
        mydb.commit()

        sql10 = "UPDATE beatdowns SET fng_count = 5 where fngs like '5%' AND fng_count IS NULL"
        cursor.execute(sql10)
        mydb.commit()
finally:
    pass
logging.info("Beatdown execution complete for region " + db)

mydb.close()

pm_log_text += "End of PAXminer hourly run"
try:
    slack.chat_postMessage(channel='paxminer_logs', text=pm_log_text)
except:
    print("Slack log message error - not posted")
    pass
print('Finished. You may go back to your day!')