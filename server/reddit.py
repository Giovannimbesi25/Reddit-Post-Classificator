import praw
from flask import Flask, request, redirect, json

reddit = praw.Reddit(
    client_id="0thB8dNY6QTqf53ab09HXw",
    client_secret="Ex-B3JebUCpNAfsUffrgGofkL701UQ",
    user_agent="tapApp",
        username="giovannImbs",
    password="Dragon25",

)

red_server =  Flask(__name__)


    
#http://192.168.137.221:5000/streaming
#parole più utilizzate con cluster
#parole più grandi in base alla loro importanza
#associate a tag to every title submission
#big tagg, big amount of them

@red_server.route('/streaming')
def streming():
    subreddit = reddit.subreddit("AskReddit")
    for submission in subreddit.new(limit=1):
        return red_server.response_class(
        response=json.dumps({ "title": submission.title, "score" : submission.score }),
        mimetype='application/json'
        )
            



if __name__ == "__main__":
    red_server.run(debug=True,
            host='0.0.0.0',
            port=5000)
    
    





    