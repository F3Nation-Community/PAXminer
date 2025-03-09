# CHARTS

## Chart Types
### AO 
- Controlled by the send_ao_leaderboard flag in the region database.
- LeaderboardByAO_Charter.py
- Two Graphs Sent
    - PAX posts in the last month
    - PAX posts YTD

### Q
- Controlled by the send_q_charts flag in the database.
- QCharter.py
- Q's in the last month to each AO
- Q's in the last month to the firstf channel, bisected by AO.

### Region
- Controlled by the send_region_leaderboard flag in the database
- Leaderboard_Charter.py
- Two Graphs Sent to the firstf channel
    - PAX posts in the last month
    - PAX posts YTD

### PAX
- Controlled by the send_pax_charts flag in the database
- PAXcharter.py
- One graph sent to each PAX.
    - Monthly posting summary bisected by AO with a total in the upper right.