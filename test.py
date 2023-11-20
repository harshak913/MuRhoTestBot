import sqlite3

# Get events_url from links table
conn = sqlite3.connect('database.db')
cur = conn.cursor()
cur.execute("UPDATE links SET roster_url='https://docs.google.com/spreadsheets/d/1SlsQzigTr3gesOFz5HEni3wEE-AZnqB71HpgfpeqsfI/edit#gid=1671136875'")
conn.commit()

# Get roster_url from links table
cur.execute("SELECT roster_url FROM links")
roster_url = cur.fetchone()
print(roster_url)
