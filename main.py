# Script réalisé par Rayan-Charles Ridouard
# L'objectif de ce script est de récupérer l'ensemble des postes sur Product Hunt 
# Ce script a été réalisé à des fin de recherche dans le cadre de mon mémoire de fin d'études
 

import requests
from datetime import datetime, timedelta
import time
import json
import csv
import datetime as dt
import sqlite3


DAY_TO_SCRAP = 730
TOKEN = YOUR_TOKEN 



def graphQL(date, date_end, after="null"):
    return """
        {posts(postedAfter: \""""+date+"""\", postedBefore:  \""""+date_end+"""\", first: 20,after:\""""+after+"""\", order: NEWEST) {
            totalCount
            pageInfo {
                endCursor
                hasNextPage
            }
            nodes {
                    id
                    createdAt
                    name
                    tagline
                    description
                    topics {
                        nodes {
                            id
                            name
                            slug
                        }
                    }
                    url
                    commentsCount
                    reviewsRating
                    website
                    reviewsCount
                    votesCount
                    slug
                    thumbnail {
                        url
                    }
                }
            }

        }
    """


headers = {'Authorization': "Bearer " + TOKEN}
base_url = 'https://api.producthunt.com/v2/api/graphql'
datas = []
topics = []
topics_link = []
totalScraped = 0

date_end = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
date_start = (datetime.now() +timedelta(days=-(DAY_TO_SCRAP-1))).strftime('%Y-%m-%d 00:00:00')

do = True
cursor = "null"
last_date = ""



def main():
    global cursor, do, datas, topics, topics_link, totalScraped, last_date

    while(do):
        resp = requests.post(base_url, headers=headers, json={
            'query': graphQL(date_start, date_end, cursor)})
        response_headers = resp.headers
        if 'X-Rate-Limit-Remaining' in response_headers and int(response_headers['X-Rate-Limit-Remaining']) <= 0:
            print("{1} - Rate limit reach, waiting. Please wait {0} second(s) (total_scraped_until_now={2}, restarting_at={3}, last_date={4})".format(str(int(response_headers['X-Rate-Limit-Reset'])+60), str(
                datetime.now()), str(totalScraped), (datetime.now() + timedelta(seconds=(int(response_headers['X-Rate-Limit-Reset'])+60))).strftime('%Y-%m-%d %H:%M:%S'),last_date))
            time.sleep(int(response_headers['X-Rate-Limit-Reset'])+60)
            print("Restarting")
            continue

        response_json = resp.json()

        posts = response_json['data']['posts']['nodes']

        for post in posts:
        
            data_posts = (post["id"],)
            res_posts = cur.execute('SELECT * FROM posts WHERE id=?', data_posts)
            if res_posts.fetchone() is not None:
                continue
            data_insert = (int(post["id"]), post["name"], post["slug"], post["createdAt"],  int(post["reviewsRating"]), int(post["reviewsCount"]), int(post["commentsCount"]), int(post["votesCount"]),  post["tagline"], post["description"] if post["description"] is not None else "", post["website"],)
            cur.execute("INSERT INTO posts(id,name,slug,createdAt,reviewsRating,reviewsCount,commentsCount,votesCount,tagline,description,website) VALUES (?,?,?,?,?,?,?,?,?,?,?)",data_insert)
            totalScraped += 1
            last_date = post["createdAt"]
            for topic in post['topics']['nodes']:
                res_topics = cur.execute(
                    'SELECT * FROM topics WHERE id=?', (int(topic["id"]),))
                if res_topics.fetchone() is None:
                    cur.execute("INSERT INTO topics(id,name,slug) VALUES (?,?,?)",  (int(topic["id"]),topic["name"],topic["slug"]))
                cur.execute("INSERT INTO posts_topics(post_id,topic_id) VALUES (?,?)", (int(post["id"]), int(topic["id"])))

        con.commit()
        if response_json["data"]["posts"]["pageInfo"]["hasNextPage"] == True:
            cursor = response_json["data"]["posts"]["pageInfo"]["endCursor"]
        else:
            do = False
    
    
if __name__ == "__main__":
    try:
        con = sqlite3.connect("producthunt.db")
        cur = con.cursor()
        main()
        con.close()
        print("Finish")

    except KeyboardInterrupt:
        print("Last cursor was :" + cursor)
        print("Quitting")
        con.close()
        pass
