import datetime
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
        content = list(self.build_image_objects(subreddit_json))
        images = self.filter_images_from_content(content)
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
        self.upload_new_images(new_images)
        self.update_old_images(old_images)
        self.update_existing_image_set_file(images)


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
            elif image.is_in_db():
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
            # print(post["url"])
            # print(post["thumbnail"])
            # print(bool("preview" in post))
            post_url = post["url"]
            post_votes = post["score"]
            post_title = post["title"]
            post_timestamp_utc = post["created_utc"]
            post_comments = post["num_comments"]
            subreddit_size = post["subreddit_subscribers"]
            # print(json.dumps(post, indent=2))
            yield Image(post_title, post_url, post_timestamp_utc, post_votes, post_comments, self.subreddit, subreddit_size, post)

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


    def upload_new_images(self, images):
        for image in images:
            try:
                image.upload_image()
                print(image.subreddit)
                print(image.votes)
                if image.should_post_to_instagram():
                    print('trying to post')
                    image.post_to_instagram()

            except FileNotFoundError: # is this really necessary
                print("File not found for image {}".format(image.id))
                continue

    def update_old_images(self, old_images):
        for image in old_images:
            image.update_image() # can we get much simpler? yes -- with a lambda

    def update_existing_image_set_file(self, images):
        current_dir = get_current_dir()

        with open("{}/{}/last_files.txt".format(current_dir, self.subreddit), 'w+') as f:
            for image in images:
                f.write(image.id + os.linesep)


class Image():
    """Image class that stores metadata about an image and its ability to be scraped"""
    def __init__(self, title, url, timestamp, votes, num_comments, subreddit, subreddit_size, post_json):
        self.title = title
        self.url = url
        self.created = timestamp
        self.votes = votes
        self.subreddit = subreddit
        self.subreddit_size = subreddit_size
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
                'Key': 'url',
                'Value': self.url,
            },
            {
                'Key': 'content_source',
                'Value': RedditScraper.get_source()
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
        self._put_dynamodb(object_name)

    def update_image(self):
        client = boto3.client('dynamodb')
        response = client.get_item( # switch to batch get item to optimize DB calls
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}}
        )

        if 'Item' not in response:
            print('Image we want to update does not exist in the database yet, id=' + self.id) 
            self.upload_image()
            return


        # Get size of engagement log
        n = len(response['Item']['engagement']['M']['timestamps']['L'])

        client.update_item(
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}},
            UpdateExpression=
                'SET engagement.scores[{N}] = :s, engagement.num_comments[{N}] = :c, engagement.#ts[{N}] = :t, current_score = :s, current_num_comments = :c'
                .format(N=n),
            ExpressionAttributeNames={
                "#ts": "timestamps"
            },
            ExpressionAttributeValues={
                ":s": {"N": str(self.votes)},
                ":c": {"N": str(self.comments)},
                ":t": {"N": str(time.time())}
            }
        )

    def is_in_db(self):
        client = boto3.client('dynamodb')
        response = client.get_item(
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}}
        )
        # This is the condition for 'presence' in the database
        return 'Item' in response


    # These requirements will change over time - see analyze.py file for testing 
    def should_post_to_instagram(self):
        time_since_posted = time.time() - self.created
        if time_since_posted > 10000:
            return False

        early_ups_ratio = self.votes / time_since_posted

        return self.subreddit.lower() == "programmerhumor" and self.early_ups_ratio > .012


    def post_to_instagram(self):
        account_name = secrets.ACCOUNT_NAME_FOR_SUBREDDIT[self.subreddit]
        account_password = secrets.ACCOUNT_PASSWORD_FOR_SUBREDDIT[self.subreddit]

        # simply have to use the image path as the data for the form Image element in the post request (if this were a cURL)
        current_dir = get_current_dir()
        with open("{dir}/{sub}/images/{f}".format(
            dir=current_dir, 
            sub=self.subreddit, 
            f=self.id), 'rb') as image_file:
            form_file = {
                'image': image_file
            }
            hashtags = secrets.HASHTAGS_FOR_SUBREDDIT[self.subreddit]
            caption = self.title + "\n.\n.\n" + hashtags
            payload = {
                'caption': caption,
                'username': account_name,
                'password': account_password # or just give user auth_token or something
            }
            requests.post(secrets.API_URL + '/instant', files=form_file, data=payload)

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

    def _put_dynamodb(self, object_name):
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

        client.put_item(
            TableName=CURRENT_TABLE,
            Item={
                'community': {'S': self.subreddit},
                'community_size': {'N': str(self.subreddit_size)},
                'content_source': {'S': RedditScraper.get_source()},
                'created': {'N': str(self.created)},
                'current_score': {'N': str(self.votes)},
                'current_num_comments': {'N': str(self.comments)},
                's3_key': {'S': object_name},
                's3_bucket': {'S': CURRENT_BUCKET},
                'image_url': {'S': self.url},
                'id': {'S': self.id}, # Primary Key
                'associated_text': {'S': self.title},
                'engagement': {
                    'M': {
                        "timestamps": {
                            "L": [{'N': str(time.time())}]
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
