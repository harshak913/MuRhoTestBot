import os
from slack_sdk.signature import SignatureVerifier
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from pathlib import Path
from slackeventsapi import SlackEventAdapter
from flask import Flask, request, Response
import gspread
import numpy as np
import pandas as pd
from datetime import datetime
import re
import sqlite3
from dadjokes import Dadjoke
from apscheduler.schedulers.background import BackgroundScheduler
from better_profanity import profanity
import randfacts

#Load the token from .env & authenticate access to the Slack bot app
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

#Authenticate access to the Google Sheet
sa = gspread.service_account(filename='service_account.json')

def open_connection():
    connection = sqlite3.connect("database.db")
    return connection

#Initialize the Slack client
client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
BOT_ID = client.api_call("auth.test")['user_id']

#Define SQLite database
conn = open_connection()
c = conn.cursor()

#Create a links table that will store the links to the Google Sheet
c.execute('''CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, events_url TEXT, roster_url TEXT, budget_url TEXT)''')
conn.commit()
conn.close()

#Initialize the Flask app & Slack event adapter
app = Flask(__name__)
slack_events_adapter = SlackEventAdapter(os.getenv('SLACK_SIGNING_SECRET'), "/slack/events", app)

processed_messages = set()

@slack_events_adapter.on("message")
def message(payload):
    #Extract all the necessary information from the payload
    event = payload.get("event", {})
    channel_id = event.get("channel")
    user_id = event.get("user")
    text = event.get("text")
    message_id = event.get("ts")
    
    #Ignore messages from the bot itself
    if BOT_ID != user_id:

        #Check if the message has already been processed
        if message_id in processed_messages:
            return
        processed_messages.add(message_id)

        #Check if the message contains all possible keywords for 'upcoming events' in the user request
        if in_list(text.lower(), ['upcoming event', 'upcoming chapter event', 'events coming up', 'future event', 'future chapter event']):
            #Let the user know the bot is working on the request
            send_chat_message(channel=channel_id, text="Give me a few seconds to fetch the data...")
            send_chat_message(channel=channel_id, text=upcoming_events())
        elif in_list(text, ['update the events calendar', 'update events calendar', 'update calendar', 'update calendar of event', 'update event']):
            if 'Harsha' in client.users_info(user=user_id)['user']['profile']['real_name']:
                db_logic("events_url", text)
                send_chat_message(channel=channel_id, text="Updated the events calendar link for you :simple_smile:\nAll user queries will now use this updated link!")
            else:
                send_chat_message(channel=channel_id, text="Sorry, you don't have permission to update the events calendar :white_frowning_face:")
        elif 'slay' in text.lower(): #and 'Hira' in client.users_info(user=user_id)['user']['profile']['real_name']:
            send_chat_message(channel=channel_id, text="AUR NAURRR SLAYYY :fire::fire::fire:")
        elif in_list(text, ['how many', 'which', 'what']) and in_list(text, ['requirements', 'credits']):
            if in_list(text, ['have', 'need', 'completed', 'done']):
                #Let the user know the bot is working on the request
                send_chat_message(channel=channel_id, text="Give me a few seconds to fetch the data...")
                send_chat_message(channel=channel_id, text=needed_requirements(user_id))
        elif in_list(text, ['update the roster', 'update roster', 'update brother list', 'update brother roster']):
            if 'Harsha' in client.users_info(user=user_id)['user']['profile']['real_name']:
                db_logic("roster_url", text)
                send_chat_message(channel=channel_id, text="Updated the roster link for you :simple_smile:\nAll user queries will now use this updated link!")
            else:
                send_chat_message(channel=channel_id, text="Sorry, you don't have permission to update the roster :white_frowning_face:")
        elif in_list(text, ['chapter zoom', 'zoom link', 'zoom meeting']):
            send_chat_message(channel=channel_id, text=f"Here's the link to the chapter Zoom:\n\n {chapter_zoom()}")
        elif in_list(text, ['mailtime', 'mail time']):
            send_chat_message(channel=channel_id, text=f"Here's the link to the mailtime form: https://bit.ly/mailtimeforms")
        elif in_list(text, ['update the budget', 'update budget', 'update the chapter budget', 'update chapter budget']):
            if 'Harsha' in client.users_info(user=user_id)['user']['profile']['real_name']:
                db_logic("budget_url", text)
                send_chat_message(channel=channel_id, text="Updated the budget link for you :simple_smile:\nAll user queries will now use this updated link!")
            else:
                send_chat_message(channel=channel_id, text="Sorry, you don't have permission to update the events calendar :white_frowning_face:")
        elif in_list(text, ['budget', 'budget spreadsheet', 'budget sheet', 'budget google sheet']):
            send_chat_message(channel=channel_id, text=f"{budget_sheet()}")
        elif in_list(text, ['what', 'where', 'when']) and in_list(text, ["today's event", "todays event", "today's events", "todays events", "today's chapter event", "today's chapter event", "todays chapter event", "today"]):
            send_chat_message(channel=channel_id, text=todays_event())
        elif in_list(text, ['how many', 'which', 'what']) and in_list(text, ['ritual', 'tradition']):
            #Let the user know the bot is working on the request
            send_chat_message(channel=channel_id, text="Give me a few seconds to fetch the data...")
            send_chat_message(channel=channel_id, text=ritual_attendance(user_id))
        elif in_list(text, ['how many', 'which', 'what']) and in_list(text, ['chapter meeting', 'meeting', 'chapter event', 'event']) and any(x in text.lower() for x in ['missed', 'attended', 'gone to', 'shown up', 'showed up']):
            #Let the user know the bot is working on the request
            send_chat_message(channel=channel_id, text="Give me a few seconds to fetch the data...")
            send_chat_message(channel=channel_id, text=chapter_attendance(user_id))
        elif in_list(text, ['thank', 'thanks', 'thx', 'ty']):
            #Send a reply saying you're welcome, with the user's name
            send_chat_message(channel=channel_id, text="You're welcome!! <@%s> :smile:" % user_id)
        elif profanity.contains_profanity(text.lower()):
            send_chat_message(channel=channel_id, text="Please refrain from using that language. I'm only trying to help :face_with_symbols_on_mouth:")
        elif 'joke' in text.lower():
            dadjoke = Dadjoke()
            send_chat_message(channel=channel_id, text=dadjoke.joke)
        #Tell me a story functionality
        elif in_list(text, ['random fact', 'a fact', 'fact']):
            send_chat_message(channel=channel_id, text=randfacts.get_fact())
        elif in_list(text, ['bye', 'goodbye', 'cya', 'see ya', 'see you', 'later', 'adios', 'farewell']):
            #Send a reply saying goodbye, with the user's name
            send_chat_message(channel=channel_id, text="Goodbye!! <@%s> :wave:" % user_id)
        elif in_list(text, ['how are you', 'how are u', 'how r u', 'how you doin', 'how u doin']):
            #Send a reply saying you're doing well, with the user's name
            send_chat_message(channel=channel_id, text="I'm doing well, thanks for asking!! <@%s> :smile:" % user_id)
        elif in_list(text, ['hi', 'hello', 'howdy', 'hola', 'hey']):
            #Send a reply saying hello, with the user's name
            send_chat_message(channel=channel_id, text="Hello!! <@%s> :wave: How can I help?" % user_id)
        else:
            send_chat_message(channel=channel_id, text="Sorry, I don't understand that command :white_frowning_face:")

#Define the function when the user types /help
@app.route('/help', methods=['POST'])
def help():
    data = request.form
    channel_id = data.get('channel_id')
    send_chat_message(channel=channel_id, text='Here\'s the link to the user documentation: https://docs.google.com/document/d/11AJg75hrNBqvMluzdIpBV0ZRmww6LCLqcMITz1c6KFA/edit?usp=sharing')
    return Response(), 200

#Define the function to return service requirements for all brothers
def needed_requirements(user_id):
    #Grab the roster url from the database
    roster_url = roster_ws()[0]
    worksheets = roster_ws()[1]
    
    indices = []
    #Return the indices of all worksheets that contain the keywords
    for worksheet in worksheets:
        if any(x in worksheet.title.lower() for x in ['service', 'professional', 'fundraising', 'rush']):
            indices.append(worksheets.index(worksheet))

    response = ""

    #Iterate through all worksheets that contain the keywords
    for index in indices:
        #Get the req_list
        req_list = roster_df(roster_url, index, worksheets, user_id)[0]
        headers = roster_df(roster_url, index, worksheets, user_id)[1]

        if req_list.empty:
            return "Sorry, I couldn't find your name in the roster :white_frowning_face:"
        
        #Get requirement as listed in the header
        try:
            requirement = headers[1].split('(')[1].rstrip(')')
        except:
            requirement = "N/A"
            
        response += f"*{worksheets[index].title.split()[0]} requirements needed:* {requirement}\n"

        #If the user has not completed the requirements, return a string saying so
        if req_list.iloc[0,1] == 0:
            response += f"\nYou have *NOT* completed any {worksheets[index].title.split()[0]} requirement(s) for this semester yet!"
        else:
            response += f"\nYou have completed *{req_list.iloc[0, 1]} {worksheets[index].title.split()[0].lower()} requirement(s)* for this semester! Here are the {worksheets[index].title.split()[0].lower()} events you have completed:\n"
            for col in req_list.columns[2:]:
                if req_list.iloc[0, req_list.columns.get_loc(col)] == 'TRUE':
                    response += f"- {col}\n"
            response += "\n\n"
    response += "*Disclaimer:* \n- The requirements are assuming you are an active brother. If you are PT LOA, please reach out to the VPO to confirm your requirements.\n"
    response += "- If you are missing any requirements that you have already fulfilled, please reach out to the VPO. There may be discrepancies because I pull data from the roster, which may not be up-to-date yet :simple_smile:"
    return response

#Define the function to return the upcoming events
def upcoming_events():
    #Grab the events url from the database
    conn = open_connection()
    c = conn.cursor()
    events_url = c.execute('''SELECT events_url FROM links''').fetchone()[0]
    conn.close()

    #Extract the upcoming events from the Google Sheet
    events_calendar = sa.open_by_url(events_url).worksheet("Semester Calendar")

    #Grab all events from the Google Sheet and convert first 6 columns into a numpy array, ignoring the first 3 rows
    events_list = np.array(events_calendar.get_all_values()[3:])[:,:6]

    #Grab the first row of the Google Sheet as indices for the events list
    indices = events_calendar.get_all_values()[0][:6]

    #Convert the numpy array into a pandas DataFrame using events_list as the data and indices as the column names
    events_list = pd.DataFrame(data=events_list, columns=indices)

    # Drop all rows with empty cells
    events_list = events_list.dropna()

    #Convert the month from name to a zero-padded number
    events_list['Month'] = events_list['Month'].apply(lambda x: datetime.strptime(x, '%B').strftime('%m'))

    #Add the date & strip the last 2 characters & convert to a zero-padded number
    events_list['Date'] = events_list['Date'].apply(lambda x: x[:-2].zfill(2))

    # Add the current year to the date column
    events_list['Date'] = f"{events_list['Month']}/" + events_list['Date'] + f'/{datetime.now().year}'
    # current_date = datetime.now().strftime("%m/%d/%Y")
    current_date = '09/12/2023'

    #Filter the events list to only include events that have not passed yet
    events_list = events_list[events_list['Date'] >= current_date]

    if events_list.empty:
        return "According to the events calendar, there are no upcoming events."

    #Reset indices from 0
    events_list.reset_index(drop=True, inplace=True)

    #Construct the response to the user only according to whether or not certain fields are empty
    response = f"Here are the upcoming events, according to the events calendar:\n\n"
    for index in events_list.index:
        response += f"- {events_list.iloc[index, 3].strip()} on *{events_list.iloc[index, 1].strip()}*"
        if events_list.iloc[index, 2] != '':
            response += f" at *{events_list.iloc[index, 2].strip()}*"
        if events_list.iloc[index, 4] != '':
            if 'Zoom' in events_list.iloc[index, 4]:
                response += " on "
            else:
                response += " at "
        response += f"{events_list.iloc[index, 4].strip()}\n"
    return response

#Return the chapter zoom link from pinned messages
def chapter_zoom():
    link = '*Join Zoom Meeting*\n'
    link += 'Meeting ID: 424 105 2010\n'
    link += 'Password: akpsimurho\n'
    link += 'https://us02web.zoom.us/j/4241052010?pwd=d3JKMC9kUHhRZnVmN0ZLR1VrTXRqUT09'

    return link
        
def ritual_attendance(user_id):
    #Grab the roster url from the database
    roster_url = roster_ws()[0]
    worksheets = roster_ws()[1]

    response = ""
    attended = ""
    missed = ""
    
    index = None
    #Return the indices of all worksheets that contain the keywords
    for worksheet in worksheets:
        if 'ritual' in worksheet.title.lower():
            index = worksheets.index(worksheet)
            break
    
    if index == None:
        return "Sorry, I couldn't find the ritual attendance sheet :white_frowning_face:"
    
    req_list = roster_df(roster_url, index, worksheets, user_id)[0]

    if req_list.empty:
        return f"Sorry, I couldn't find your name in the roster :white_frowning_face:"
    
    response += f"Here's how many ritual absences you have this semester: *{req_list.iloc[0, 1]}*\n\n"
    attended += "You have *attended* the following events:\n"
    missed += "You have *missed* the following events:\n"
    #Loop through each column in the dataframe and add the events that the user has completed to the response
    for col in req_list.columns[2:]:
        if req_list.iloc[0, req_list.columns.get_loc(col)] == 'TRUE':
            attended += f"- {col}\n"
        elif req_list.iloc[0, req_list.columns.get_loc(col)] == 'FALSE':
            missed += f"- {col}\n"
    
    response += f"\n{attended}\n{missed}\n\n"
    response += "*Disclaimer:* \n- If you are PT LOA, please reach out to the VPO to confirm your ritual attendance requirements.\n"
    response += "- If a ritual you have attended is counted as an absence, please reach out to the VPO. There may be discrepancies because I pull data from the roster's ritual attendance sheet, which may not be up-to-date yet :simple_smile:"

    return response

def chapter_attendance(user_id):
    #Grab the roster url from the database
    roster_url = roster_ws()[0]
    worksheets = roster_ws()[1]
    
    index = None
    #Return the indices of all worksheets that contain the keywords
    for worksheet in worksheets:
        if 'chapter attendance' in worksheet.title.lower():
            index = worksheets.index(worksheet)
            break

    if index == None:
        return "Sorry, I couldn't find the ritual attendance sheet :white_frowning_face:"
    
    req_list = roster_df(roster_url, index, worksheets, user_id)[0]

    if req_list.empty:
        return f"Sorry, I couldn't find your name in the roster :white_frowning_face:"
    
    response = f"Here's how many absences you have this semester for required chapter meetings & events: *{req_list.iloc[0, 1]}*\n\n"
    missed = "You have *missed* the following required meetings & events:\n"

    #Loop through each column in the dataframe and add the events that the user has missed to the response
    for col in req_list.columns[2:]:
        if req_list.iloc[0, req_list.columns.get_loc(col)] == 'FALSE':
            missed += f"- {col}"
            #Check if the column header has a '/'
            if '/' in col:
                missed += " chapter meeting\n"
            else:
                missed += "\n"
    
    response += f"{missed}\n\n"
    response += "*Disclaimer:* \n- If you are PT LOA, please reach out to the VPO to confirm your chapter attendance requirements.\n"
    response += "- If a chapter meeting/event you have attended is counted as an absence, please reach out to the VPO. There may be discrepancies because I pull data from the roster's chapter attendance sheet, which may not be up-to-date yet :simple_smile:"

    return response

#Automatically send a user "Happy Birthday" based on the roster when the date hits
def birthday():
    #Grab the roster url from the database
    roster_url = roster_ws()[0]
    worksheets = roster_ws()[1]

    index = None
    #Return the index of the worksheet that contains all active brothers
    for worksheet in worksheets:
        if any(x in worksheet.title.lower() for x in ['active brother', 'active member']):
            index = worksheets.index(worksheet)
            break
    
    #Grab today's date
    today = datetime.today().strftime('%b %-d')
    #today = 'Sep 13'

    #Open the worksheet
    current_worksheet = sa.open_by_url(roster_url).worksheet(worksheets[index].title)
    #Grab all brothers' names and requirements from the Google Sheet and convert all columns into a numpy array, ignoring the first row
    brothers_list = np.array(current_worksheet.get_all_values()[1:])
    headers = current_worksheet.get_all_values()[0]
    headers = list(filter(lambda item: item != "", headers))
    req_list = brothers_list[:,:len(headers)]

    #Convert the numpy array into a pandas DataFrame using brothers_list as the data and headers as the column names
    req_list = pd.DataFrame(data=req_list, columns=headers)
    req_list = req_list.iloc[:, [0,1,4]]

    #Format all birthdays to be the format of today
    req_list.iloc[:, 2] = req_list.iloc[:, 2].apply(format_birthday)

    #Grab all birthdays that match the current date
    req_list = req_list[req_list.iloc[:, 2] == today]

    #If the birthday is empty, return a string saying so
    if req_list.empty == '':
        return
    else:
        #Iterate through every row in the df
        for index, row in req_list.iterrows():
            user_id = None

            #Make a string of the 1st & 2nd column combined
            name = row[0].lower() + ' ' + row[1].lower()

            #Get the user's user_id based on their name
            user_list = client.users_list()['members']
            for user in user_list:
                if user['profile']['real_name_normalized'].lower() == name:
                    user_id = user['id']
                    break
            #Check if the user_id is None
            if user_id == None:
                return
            else:
                #Send the user a message
                send_dm_message(user_id, f"Happy Birthday, <@{user_id}>! :tada: :birthday:")

#Tell the user when & where today's event is
def todays_event():
    #Grab the events url from the database
    conn = open_connection()
    c = conn.cursor()
    events_url = c.execute('''SELECT events_url FROM links''').fetchone()[0]
    conn.close()

    #Extract the upcoming events from the Google Sheet
    events_calendar = sa.open_by_url(events_url).worksheet("Semester Calendar")

    #Grab all events from the Google Sheet and convert first 6 columns into a numpy array, ignoring the first 2 rows
    events_list = np.array(events_calendar.get_all_values()[2:])[:,:6]

    #Grab the first row of the Google Sheet as indices for the events list
    indices = events_calendar.get_all_values()[0][:6]

    #Convert the numpy array into a pandas DataFrame using events_list as the data and indices as the column names
    events_list = pd.DataFrame(data=events_list, columns=indices)

    #Add the current year to the date column
    current_date = datetime.now().strftime("%m/%d")

    #Grab today's event
    events_list = events_list[events_list.iloc[:, 1] == current_date]

    response = ''

    #If the event is empty, return a string saying so
    if events_list.empty:
        return f"There are no events today :white_frowning_face:"
    elif len(events_list) > 1:
        response += f"There are *{len(events_list)}* events today, according to the events calendar:\n\n"
    else:
        response += f"There is *{len(events_list)}* event today, according to the events calendar:\n\n"

    for index, row in events_list.iterrows():
        #Grab the event's name, time, and location
        event_name = row[3]
        event_time = row[2]
        event_location = row[4]

        #Return a string with the event's name, time, and location
        response += f"*Event:* {event_name}\n"

        #Check if the event has a time
        if event_time == '':
            response += "No time has been provided on the events calendar. Please check with the VPO.\n"
        else:
            response += f"*Time:* {event_time}\n"

        #Check if the event has a location
        if event_location == '':
            response += "No location has been provided on the events calendar. Please check with the VPO.\n\n"
        else:
            response += f"*Location:* {event_location}\n\n"
    
    return response

def send_chat_message(channel, text):
    client.chat_postMessage(channel=channel, text=text)

def send_dm_message(user_id, text):
    response = client.conversations_open(users=[user_id])
    channel_id = response["channel"]["id"]
    client.chat_postMessage(channel=channel_id, text=text)

def in_list(text, keywords):
    return any(x in text.lower() for x in keywords)

#Refactor events_url and roster_url insertion into database into a single function
def db_logic(column_to_update, text):
    conn = open_connection()
    c = conn.cursor()
    c.execute(f'''UPDATE links SET {column_to_update} = "{text}"''')

    #Check if id = 1 exists in the links table
    c.execute('''SELECT * FROM links WHERE id = 1''')
    if c.fetchone() is None:
        #Insert a new row into the links table
        c.execute(f'''INSERT INTO links ({column_to_update}) VALUES ("{text.split(':')[1].replace('<', '').strip() + text.split(':')[2].replace('>', '').strip()}")''')
    else:
        #Update the events calendar link in the database for the entry id = 1
        c.execute(f'''UPDATE links SET {column_to_update} = "{text.split(':')[1].replace('<', '').strip() + text.split(':')[2].replace('>', '').strip()}" WHERE id = 1''')
    conn.commit()
    conn.close()

#Refactor roster dataframe creation into a single function
def roster_df(roster_url, index, worksheets, user_id):

    #Open the worksheet
    current_worksheet = sa.open_by_url(roster_url).worksheet(worksheets[index].title)
    #Grab all brothers' names and requirements from the Google Sheet and convert all columns into a numpy array, ignoring the first row
    brothers_list = np.array(current_worksheet.get_all_values()[1:])
    headers = current_worksheet.get_all_values()[0]
    headers = list(filter(lambda item: item != "", headers))
    req_list = brothers_list[:,:len(headers)]

    #Convert the numpy array into a pandas DataFrame using brothers_list as the data and headers as the column names
    req_list = pd.DataFrame(data=req_list, columns=headers)

    #Filter the req list according to the user's name (from user id)
    first_name = client.users_info(user=user_id)['user']['profile']['real_name'].split()[0].strip()
    req_list = req_list[req_list.iloc[:, 0].str.lower().str.contains(first_name.lower(), flags=re.IGNORECASE, regex=True)]

    # If the worksheet name has 'chapter attendance' in it, return the dataframe with the last column dropped
    if 'chapter attendance' in worksheets[index].title.lower():
        req_list = req_list.iloc[:, :-1]
    elif 'ritual' in worksheets[index].title.lower():
        req_list = req_list.iloc[:, :-1]
    
    #If the dataframe has more than 1 entry:
    if len(req_list) > 1:
        #Filter the req list according to the first letter of the user's last name
        last_name = client.users_info(user=user_id)['user']['profile']['real_name'].split()[1].strip()
        req_list = req_list[req_list.iloc[:, 0].str.lower().str.contains(last_name[0].lower(), flags=re.IGNORECASE, regex=True)]
    
    return req_list, headers

#Refactor accessing roster worksheets into a single function
def roster_ws():
    #Grab the roster url from the database
    conn = open_connection()
    c = conn.cursor()
    roster_url = c.execute('''SELECT roster_url FROM links''').fetchone()[0]
    conn.close()
    return roster_url, sa.open_by_url(roster_url).worksheets()

#Format the birthday to be the format of today
def format_birthday(date):
    date = date.split()
    if len(date) == 0 or len(date) == 1:
        return ''
    birthday = f"{date[0][0:3]}"
    if len(date[1]) == 3:
        birthday += f" {date[1][0]}"
    else:
        birthday += f" {date[1][0:2]}"
    return birthday

def budget_sheet():
    #Grab the budget url from the database
    conn = open_connection()
    c = conn.cursor()
    budget_url = c.execute('''SELECT budget_url FROM links''').fetchone()[0]
    conn.close()
    if budget_url is None:
        return 'I don\'t have access to the budget sheet. Please check with the VPF.'
    else:
        budget_url = budget_url.split('https')
        budget_url = 'https:' + budget_url[1]
        return f'Here\'s the link to the chapter budget: {budget_url}'

# Create a background scheduler
scheduler = BackgroundScheduler()

# Define the scheduled tasks
scheduler.add_job(birthday, trigger="cron", hour=9, minute=00)

# Start the scheduler
scheduler.start()

#Concurrently run the schedule and the flask app
if __name__ == "__main__":
    app.run(debug=True)