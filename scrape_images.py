import hashlib
import json
import os
import re
import sys
import time
#custom
import boto3
import click
import requests
import wget

import secrets

client_id = secrets.CLIENT_ID
client_secret = secrets.CLIENT_SECRET

client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

USER_AGENT_STR = 'request:from:meme:scraper:project:by:evan'

CURRENT_BUCKET = 'reddit-memes'
CURRENT_TABLE = 'meme-metadata'
SUBREDDIT = None

def authorize_reddit():
    # Application-only authorization
    r = requests.post(
        'https://www.reddit.com/api/v1/access_token', 
        auth=client_auth,
        data={
            'username': secrets.USERNAME,
            'password': secrets.PASSWORD,
            'grant_type':'password'
        },
        headers={
            'User-Agent': USER_AGENT_STR
        }
    )

    if r.ok:
        return r.json()['access_token']

def s3tagfilter(s):
    # filters out characters that cannot be put into an S3 tag
    return re.sub(r'[^0-9a-zA-Z _]', '', s) 

def get_current_dir():
    current_dir, executing_file = os.path.split(os.path.abspath(__file__))
    return current_dir


class RedditScraper():
    """Reddit Scraper object that scrapes and stores all hot images from its subreddit."""
    def __init__(self, subreddit):
        self.subreddit = subreddit
        # Would we ever want to do multiple subreddit scrapes in this file? if so, make this global
        self.access_token = authorize_reddit()
        self.existing_image_set = self.get_existing_image_set()

    @staticmethod
    def get_source():
        return "reddit.com"

    def scrape_and_store(self, n=None):
        subreddit_json = self.get_hot_subreddit_response()
        # print(json.dumps(subreddit_json, indent=2))
        images = list(self.build_image_objects(subreddit_json))
        filtered_images = self.filter_downloadable_images(images, n)
        self.prepare_to_download_images()
        self.download_images(filtered_images)
        self.upload_images(filtered_images)
        self.update_existing_image_set(images)


    def get_existing_image_set(self):
        current_dir = get_current_dir()
        image_ids = set()
        with open("{}/{}/last_files.txt".format(current_dir, self.subreddit), 'r+') as f:
            for line in f:
                if line not in image_ids:
                    image_ids.add(line.rstrip())
        return image_ids


    # roughly one request every two seconds... at some point need to test the limits of this
    def get_hot_subreddit_response(self):
        if self.access_token is None:
            raise ValueError("Access token was not present. Either include access token or authorize before calling")
        authorized_header = {
            "Authorization": "bearer {}".format(self.access_token),
            "User-Agent": USER_AGENT_STR
        }
        response = requests.get(
            "https://oauth.reddit.com/r/{}/hot".format(self.subreddit), 
            headers=authorized_header
        )

        if not response.ok:
            raise RuntimeError("Request error :(\n{}".format(response.text))
        json_response = json.loads(response.text)
        return json_response


    def filter_downloadable_images(self, images, n):
        for image in images:
            if not image.can_download():
                print('NOT ', image.url)

        filtered_images = [image for image in images 
            if image.can_download() 
            and image.id not in self.existing_image_set]     

        return filtered_images[0:n] if n else filtered_images

    # this step filters duplicates and non-images, and builds Image objects
    def build_image_objects(self, subreddit_json):
        posts = [child["data"] for child in subreddit_json["data"]["children"]]
        for post in posts:
            # we want to filter videos, gifs, and text posts
            # so we get only images

            # images seem to be the only form of media that uses the thumbnail tag
            # so it's an image if thumbnail != ""
            # print(post["url"])
            # print(post["thumbnail"])
            # print(bool("preview" in post))
            post_url = post["url"]
            post_votes = post["score"]
            post_title = post["title"]
            post_timestamp_utc = post["created_utc"]
            post_comments = post["num_comments"]
            print(json.dumps(post, indent=2))
            yield Image(post_title, post_url, post_timestamp_utc, post_votes, post_comments, self.subreddit, post)

    def prepare_to_download_images(self):
        current_dir = get_current_dir()
        path = os.path.join(current_dir, os.path.join(self.subreddit, 'images'))
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

    # Iteration 1: wget
    #   having some throttling issues
    def download_images(self, images):
        for image in images:
            print(image.url)
            try:
                current_dir = get_current_dir()
                wget.download(image.url, out="{dir}/{sub}/images/{f}".format(
                    dir=current_dir, 
                    sub=self.subreddit, 
                    f=image.id)
                )
            except FileNotFoundError:
                continue
            time.sleep(5)



    def upload_images(self, images):

        for image in images:
            try:
                image.upload_image()
                # current_dir = get_current_dir()
                # with open("{dir}/{sub}/images/{f}".format(
                #     dir=current_dir, 
                #     sub=self.subreddit, 
                #     f=image.id), 'rb') as image_file:

                #     object_name = "{sub}/{id}".format(sub=self.subreddit, id=image.id)
                #     client.upload_fileobj(
                #         image_file,
                #         CURRENT_BUCKET,
                #         # object name (subreddit/id - eg memes/adef1223f47bc95bc95 )
                #         Key=object_name
                #     )

                #     print(image.get_tag_set())

                #     client.put_object_tagging(
                #         Bucket=CURRENT_BUCKET,
                #         Key=object_name,
                #         Tagging={
                #             'TagSet': image.get_tag_set()
                #         }
                #     )
            except FileNotFoundError:
                print("File not found for image {}".format(image.id))
                continue


    def update_existing_image_set(self, images):
        current_dir = get_current_dir()

        with open("{}/{}/last_files.txt".format(current_dir, self.subreddit), 'w+') as f:
            for image in images:        
                f.write(image.id + os.linesep)

class Image():
    """Image class that stores metadata about an image and its ability to be scraped"""
    def __init__(self, title, url, timestamp, votes, num_comments, subreddit, post_json):
        self.title = title
        self.url = url
        self.timestamp = timestamp
        self.votes = votes
        self.subreddit = subreddit
        self.post_json = post_json
        self.comments = num_comments
        self.id = hashlib.md5(self.url.encode()).hexdigest()

    def can_download(self):
        is_reddit_video = self.post_json["secure_media"] is not None and "reddit_video" in self.post_json["secure_media"]
        return self.post_json["thumbnail"] != "" \
            and "preview" in self.post_json \
            and not self.post_json["url"].startswith("https://www.reddit.com/r/") \
            and not (self.post_json["url"].endswith('gif') or self.post_json["url"].endswith('gifv')) \
            and not is_reddit_video


    def get_tag_set(self):
        return [
            {
                'Key': 'title',
                'Value': s3tagfilter(self.title),
            },
            {
                'Key': 'url',
                'Value': self.url,
            },
            {
                'Key': 'timestamp',
                'Value': str(self.timestamp),
            },
            {
                'Key': 'votes',
                'Value': str(self.votes),
            },
            {
                'Key': 'subreddit',
                'Value': self.subreddit,
            },
            {
                'Key': 'id',
                'Value': self.id,
            },
        ]

    def upload_image(self):
        object_name = "{sub}/{id}".format(sub=self.subreddit, id=self.id)
        self._load_file_in_s3_bucket(object_name)
        self._add_s3_tagging(object_name)
        self._update_dynamodb(object_name)


    def is_in_db(self, response):
        client = boto3.client('dynamodb')
        

        # This is the condition for 
        return 'Item' in response

    def _load_file_in_s3_bucket(self, object_name):
        client = boto3.client('s3')
        current_dir = get_current_dir()
        with open("{dir}/{sub}/images/{f}".format(
            dir=current_dir, 
            sub=self.subreddit, 
            f=self.id), 'rb') as image_file:

            client.upload_fileobj(
                image_file,
                CURRENT_BUCKET,
                # object name (subreddit/id - eg memes/adef1223f47bc95bc95 )
                Key=object_name
            )

    def _add_s3_tagging(self, object_name):
        client = boto3.client('s3')
        client.put_object_tagging(
            Bucket=CURRENT_BUCKET,
            Key=object_name,
            Tagging={
                'TagSet': self.get_tag_set()
            }
        )

    def _update_dynamodb(self, object_name):
        ''' fields:
        "id": self.id # Primary Key
        "s3_key": object_name
        "s3_bucket": CURRENT_BUCKET
        "title": self.title # synonymous with caption
        "url": self.url
        "engagement": {
            "timestamps": [],
            "score": [], # likes (twitter, ig), upvotes (reddit), reactions (fb)
            "num_comments": [],
        } 
        '''
        client = boto3.client('dynamodb')
        response = client.get_item(
            TableName=CURRENT_TABLE,
            Item={'id': {'S': self.id}}
        )
        print(response)
        # put item for new ones, update item for old ones
        if self.is_in_db(response):
            # update item
            num_scores = len(response['engagement']['scores'])
            # we want to insert new scores, timestamps, and comments at the back of the list

            # client.update_item(
            #     TableName=CURRENT_TABLE,
            #     Key={'id': {'S': self.id}},
            #     UpdateExpression='SET resp'
            # )
        else:
            client.put_item(
                TableName=CURRENT_TABLE,
                Item={
                    'community': {'S': self.subreddit},
                    'content_source': {'S': RedditScraper.get_source()},
                    's3_key': {'S': object_name},
                    's3_bucket': {'S': CURRENT_BUCKET},
                    'image_url': {'S': self.url},
                    'id': {'S': self.id}, # Primary Key
                    'engagement': {
                        'M': {
                            "timestamps": {
                                "L": [{'N': str(self.timestamp)}]
                            },
                            "scores": {
                                "L": [{'N': str(self.votes)}]
                            },
                            "num_comments": {
                                "L": [{'N': str(self.comments)}]
                            },
                        }
                    }

                } 
            )



@click.command()
@click.argument('subreddit', nargs=1)
@click.option('--limit', is_flag=True)
def controller(subreddit, limit):
    scraper = RedditScraper(subreddit)
    if limit:
        scraper.scrape_and_store(n=2)
    else:
        scraper.scrape_and_store()

if __name__ == '__main__':
    controller()
