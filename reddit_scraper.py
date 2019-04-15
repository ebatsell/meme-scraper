import datetime
import hashlib
import itertools
import json
import os
import re
import time
#pypi
import boto3
import requests
import wget
#local modules
import secrets
from image import Image

client_id = secrets.CLIENT_ID
client_secret = secrets.CLIENT_SECRET

client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

USER_AGENT_STR = 'request:from:meme:scraper:project:by:evan'


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
        content = list(self.build_image_objects(subreddit_json))
        images = self.filter_images_from_content(content)
        '''
        new_images, old_images = self.filter_new_images(images)
        print('new images')
        print([image.id for image in new_images])
        print('old images')
        print([image.id for image in old_images])
        # Restrict number of new images for testing so it goes faster
        if n:
            new_images = new_images[0:n]
        self.prepare_to_download_images() # currently removes image files - will want to change to keeping images only in memory
        self.download_new_images(new_images)
        self.upload_images_to_instagram(new_images, old_images) # should go before dynamo stuff
        self.store_new_images(new_images)
        self.update_old_images(old_images)
        self.update_existing_image_set_file(images)
        '''

        self.prepare_to_download_images() # currently removes image files - will want to change to keeping images only in memory

        for image in images:
            try:
                if image.in_db: # Seen image
                    if image.should_post_to_instagram():
                        image.ensure_image_downloaded()
                        image.post_to_instagram()
                    image.update_image()
                else: # New image
                    image.download_source()
                    if image.should_post_to_instagram():
                        image.post_to_instagram()
                    image.upload_image() # Handles S3 and DynamoDB posting
            except Exception as e:
                print("Image processing failed")
                print(image.id, image.url)
                print(e)


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

    def filter_images_from_content(self, content_objects):
        filtered_images = [image for image in content_objects if image.can_download()]
        return filtered_images 

    def filter_new_images(self, images):
        ''' Returns a tuple of two lists (new, old) where new is images that
        have never been seen before and old is the images that are not new. '''
        old_images = []
        new_images = []

        for image in images:
            # If they are in the existing_image_set they are guaranteed to be old
            if image.id in self.existing_image_set:
                old_images.append(image)
            elif image.in_db:
                old_images.append(image)
            else:
                new_images.append(image)

        return (new_images, old_images)

    # deprecated...
    def filter_downloadable_images(self, images, n):
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
            post_url = post["url"]
            post_votes = post["score"]
            post_title = post["title"]
            post_timestamp_utc = post["created_utc"]
            post_comments = post["num_comments"]
            subreddit_size = post["subreddit_subscribers"]
            # print(json.dumps(post, indent=2))
            yield Image(
                post_title,
                post_url,
                post_timestamp_utc,
                post_votes,
                post_comments,
                self.subreddit,
                subreddit_size,
                post
            )

    # Wipes the current images
    def prepare_to_download_images(self):
        current_dir = get_current_dir()
        path = os.path.join(current_dir, os.path.join(self.subreddit, 'images'))
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

    # Iteration 1: wget
    #   having some throttling issues - t=5s seems to do the trick
    def download_new_images(self, images):
        for image in images:
            image.download_source()

    def store_new_images(self, new_images):
        for image in new_images:
            try:
                image.upload_image()
            except FileNotFoundError: # is this really necessary
                print("File not found for image {}".format(image.id))
                continue


    def upload_images_to_instagram(self, new_images, old_images):
        for image in itertools.chain(new_images, old_images):
            if image.should_post_to_instagram():
                print('should post', image.id, image.title)
                image.ensure_image_downloaded()
                image.post_to_instagram()

    def update_old_images(self, old_images):
        for image in old_images:
            image.update_image() # can we get much simpler? yes -- with a lambda

    def update_existing_image_set_file(self, images):
        current_dir = get_current_dir()

        with open("{}/{}/last_files.txt".format(current_dir, self.subreddit), 'w+') as f:
            for image in images:
                f.write(image.id + os.linesep)
