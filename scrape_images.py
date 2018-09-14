import hashlib
import json
import os
import re
import sys
import time
#custom
import boto3
import requests
import wget
# import requests.auth
# from requests_oauthlib import OAuth2Session
# from oauthlib.oauth2 import BackendApplicationClient

client_id = 'WoCOecHciN-y8A' # secrets.CLIENT_ID
client_secret = 'Cw_UdVtez9tzxMVxEFG-wJme1SQ' # secrets.CLIENT_SECRET

client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

USER_AGENT_STR = 'request:from:meme:scraper:project:by:evan'

CURRENT_BUCKET = 'reddit-memes'
SUBREDDIT = None


def s3tagfilter(s):
    # filters out characters that cannot be put into an S3 tag
    return re.sub(r'[^0-9a-zA-Z _]', '', s)


def get_existing_image_set():
    current_dir, exec_file = os.path.split(os.path.abspath(__file__))
    image_ids = set()
    with open("{}/{}/last_files.txt".format(current_dir, sys.argv[1]), 'r+') as f:
        for line in f:
            if line not in image_ids:
                image_ids.add(line.rstrip())
    return image_ids


class Image():
    existing_image_set = get_existing_image_set()

    def __init__(self, title, url, timestamp, votes, community, post_json):
        self.title = title
        self.url = url
        self.timestamp = timestamp
        self.votes = votes
        self.community = community
        self.post_json = post_json

        self.id = hashlib.md5(self.url.encode()).hexdigest()

    def can_download(self):
        # print(self.existing_image_set)
        # print(self.post_json["thumbnail"] != "" and "preview" in self.post_json and self.id not in self.existing_image_set)

        return self.post_json["thumbnail"] != "" \
            and "preview" in self.post_json \
            and self.id not in self.existing_image_set


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


    def get_image_dir_path(self):
        pass

    def get_image_file_path(self):
        pass

def execute(subreddit):
    access_token = authorize_reddit()
    # try:
    #     with open("token", "r") as token_file:
    #         access_token = token_file.read()
    # except FileNotFoundError:
    #     access_token = authorize_reddit()
    #     with open("token", "w+") as token_file:
    #         token_file.write(access_token)

    subreddit_json = get_hot_subreddit_response(subreddit, access_token)
    images = list(build_image_objects(subreddit_json))
    filtered_images = filter_downloadable_images(images)

    download_images(filtered_images)
    # except Exception:
    upload_images(filtered_images)
    # wrap up
    update_existing_image_set(images)


def authorize_reddit():
    # Application-only authorization
    r = requests.post(
        'https://www.reddit.com/api/v1/access_token', 
        auth=client_auth,
        data={
            'username': 'epicevan', # secrets.USERNAME
            'password': 'gui  tar7', # secrets.PASSWORD
            'grant_type':'password'
        },
        headers={
            'User-Agent': USER_AGENT_STR
        }
    )

    return r.json()['access_token']


# roughly one request every two seconds... at some point need to test the limits of this
def get_hot_subreddit_response(subreddit, access_token):

    if access_token is None:
        raise ValueError("Access token was not present. Either include access token or authorize before calling")
    authorized_header = {
        "Authorization": "bearer {}".format(access_token),
        "User-Agent": USER_AGENT_STR
    }
    response = requests.get(
        "https://oauth.reddit.com/r/{}/hot".format(subreddit), 
        headers=authorized_header
    )

    if not response.ok:
        raise RuntimeError("Request error :(\n{}".format(response.text))
    json_response = json.loads(response.text)
    return json_response


# this step filters duplicates and non-images, and builds Image objects
def build_image_objects(subreddit_json):
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
   
        # else:
            # print("Not filtering")
        # one way to filter out pure text posts
        # if post["selftext_html"] is None:
        #     continue


        post_url = post["url"]
        post_votes = post["score"]
        post_title = post["title"]
        post_timestamp_utc = post["created_utc"]
        post_subreddit = post["subreddit_name_prefixed"]

        yield Image(post_title, post_url, post_timestamp_utc, post_votes, post_subreddit, post)


def filter_downloadable_images(images):
    return [image for image in images if image.can_download()]


# Iteration 1: wget
#   having some throttling issues
def download_images(images):
    print(SUBREDDIT)
    for image in images:
        print(image.url)
        try:
            current_dir, executing_file = os.path.split(os.path.abspath(__file__))
            wget.download(image.url, out="{dir}/{sub}/images/{f}".format(dir=current_dir, sub=SUBREDDIT, f=image.id))
        except FileNotFoundError:
            continue
        time.sleep(5)

def upload_images(images):
    client = boto3.client('s3')
    ''' 
    use put-object-tagging to add metadata/tagging
    be careful with tagging: i get 10 tags
    also just look into how the fuck this s3 shit works
    
    '''

    for i, image in enumerate(images):
        try:
            current_dir, executing_file = os.path.split(os.path.abspath(__file__))
            with open("{dir}/{sub}/images/{f}".format(dir=current_dir, sub=SUBREDDIT, f=image.id), 'rb') as image_file:
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

def update_existing_image_set(images):
    current_dir, exec_file = os.path.split(os.path.abspath(__file__))

    with open("{}/{}/last_files.txt".format(current_dir, SUBREDDIT), 'w+') as f:
        for image in images:        
            f.write(image.id + os.linesep)

if __name__ == '__main__':
    SUBREDDIT = sys.argv[1]
    print(SUBREDDIT)
    execute(SUBREDDIT)
