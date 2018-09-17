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
        # Would we ever want to do multiple scrapes in this file? if so, make this global
        self.access_token = authorize_reddit()
        self.existing_image_set = self.get_existing_image_set()

    def scrape_and_store(self, n=None):
        subreddit_json = self.get_hot_subreddit_response()
        images = list(self.build_image_objects(subreddit_json))
        filtered_images = self.filter_downloadable_images(images, n)

        # self.prepare_to_download_images()
        self.download_images(filtered_images)
        # except Exception:
        self.upload_images(filtered_images)
        # wrap up
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
        filtered_images = [image for image in images 
            if image.can_download() 
            and image.id not in self.existing_image_set]     
        if n:
            filtered_images = filtered_images[0:n]
        return filtered_images

    # this step filters duplicates and non-images, and builds Image objects
    def build_image_objects(self, subreddit_json):
        posts = [child["data"] for child in subreddit_json["data"]["children"]]
        # posts = posts[0:3] # for testing purposes only
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
            post_subreddit = post["subreddit_name_prefixed"]

            yield Image(post_title, post_url, post_timestamp_utc, post_votes, post_subreddit, post)

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
                wget.download(image.url, out="{dir}/{sub}/images/{f}".format(dir=current_dir, sub=self.subreddit, f=image.id))
            except FileNotFoundError:
                continue
            time.sleep(5)



    def upload_images(self, images):
        client = boto3.client('s3')

        for i, image in enumerate(images):
            try:
                current_dir = get_current_dir()
                with open("{dir}/{sub}/images/{f}".format(
                    dir=current_dir, 
                    sub=self.subreddit, 
                    f=image.id), 'rb') as image_file:
                    client.upload_fileobj(
                        image_file,
                        CURRENT_BUCKET,
                        # object name (currently URL but that might not be the best thing)
                        Key=image.id 
                    )

                    print(image.get_tag_set())

                    client.put_object_tagging(
                        Bucket=CURRENT_BUCKET,
                        Key=image.id,
                        Tagging={
                            'TagSet': image.get_tag_set()
                        }
                    )
            except FileNotFoundError:
                continue


    def update_existing_image_set(self, images):
        current_dir = get_current_dir()

        with open("{}/{}/last_files.txt".format(current_dir, self.subreddit), 'w+') as f:
            for image in images:        
                f.write(image.id + os.linesep)

class Image():
    """Image class that stores metadata about an image and its ability to be scraped"""
    def __init__(self, title, url, timestamp, votes, community, post_json):
        self.title = title
        self.url = url
        self.timestamp = timestamp
        self.votes = votes
        self.community = community
        self.post_json = post_json
        self.id = hashlib.md5(self.url.encode()).hexdigest()

    def can_download(self):
        return self.post_json["thumbnail"] != "" \
            and "preview" in self.post_json


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
                'Key': 'community',
                'Value': self.community,
            },
            {
                'Key': 'id',
                'Value': self.id,
            },
        ]

    def get_image_file_path(self):
        pass

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
