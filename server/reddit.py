import praw
import requests
from time import sleep
import csv


reddit = praw.Reddit(
    client_id="0thB8dNY6QTqf53ab09HXw",
    client_secret="Ex-B3JebUCpNAfsUffrgGofkL701UQ",
    user_agent="tapApp",
    username="giovannImbs",
    password="Dragon25",

)

url = "http://logstash:5001"
#http://192.168.137.221:5000/streaming
#parole più utilizzate con cluster
#parole più grandi in base alla loro importanza
#associate a tag to every title submission
#big tagg, big amount of them




def streming():
    old_id = ""
    new_id = ""
    while(True):
        subreddit = reddit.subreddit("AskReddit")
        for submission in subreddit.new(limit=1000):
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
        
            
        sleep(3)

streming()



    
    





    