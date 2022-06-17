import praw
import requests
from time import sleep
import csv
import os
from dotenv import load_dotenv, find_dotenv


print(load_dotenv())



reddit = praw.Reddit(
    client_id=os.getenv('client_id'),
    client_secret=os.getenv('client_secret'),
    user_agent=os.getenv('user_agent'),
    username=os.getenv('username'),
    password=os.getenv('password'),

)

url = "http://logstash:5001"



def streming():
    old_id = ""
    new_id = ""
    while(True):
        subreddit = reddit.subreddit("AskReddit")
        for submission in subreddit.new(limit=1):
            if(old_id == ""):
                try:
                    r = requests.post(url, json={'title': submission.title})
                    old_id = submission.id
                except:
                    print("Error try except")
                    sleep(3)
                    continue
            else:
                new_id = submission.id
                if( old_id != new_id):
                    r = requests.post(url, json={'title': submission.title})
                    old_id = new_id
                    print("New reddit send")
                else:
                    print("Same subreddit")
        
            
        sleep(5)

streming()



    
    





    
