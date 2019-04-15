import hashlib
import os
import time
import requests
import wget
#pypi
import boto3
#local modules
import secrets

CURRENT_BUCKET = 'reddit-memes'
CURRENT_TABLE = 'meme-metadata'

# duplicate from reddit_scraper
def get_current_dir():
    current_dir, executing_file = os.path.split(os.path.abspath(__file__))
    return current_dir

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
        self.posted = False
        self.in_db = self._is_in_db()

    def can_download(self):
        is_reddit_video = self.post_json["secure_media"] is not None and "reddit_video" in self.post_json["secure_media"]
        return self.post_json["thumbnail"] != "" \
            and "preview" in self.post_json \
            and not self.post_json["url"].startswith("https://www.reddit.com/r/") \
            and not self.post_json["url"].startswith("https://v.redd.it/") \
            and not self.post_json["url"].startswith("https://www.youtube.com/") \
            and not (self.post_json["url"].endswith('gif') or self.post_json["url"].endswith('gifv')) \
            and not is_reddit_video

    def download_source(self):
        print(self.url)
        try:
            current_dir = get_current_dir()
            wget.download(self.url, out="{dir}/{sub}/images/{f}".format(
                dir=current_dir, 
                sub=self.subreddit, 
                f=self.id)
            )
        except Exception as e:
            print(e)
        time.sleep(5)

    def get_tag_set(self):
        return [
            {
                'Key': 'url',
                'Value': self.url,
            },
            {
                'Key': 'content_source',
                'Value': 'reddit.com'
            },
            {
                'Key': 'id',
                'Value': self.id,
            },
        ]

    def upload_image(self):
        object_name = "{sub}/{id}".format(sub=self.subreddit, id=self.id)
        if self.ensure_image_downloaded():
            self._load_file_in_s3_bucket(object_name)
            self._add_s3_tagging(object_name)
            # upload to dynamodb
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
                """SET engagement.scores[{N}] = :s, engagement.num_comments[{N}] = :c, 
                engagement.#ts[{N}] = :t, current_score = :s, current_num_comments = :c, posted = :p"""
                .format(N=n),
            ExpressionAttributeNames={
                "#ts": "timestamps"
            },
            ExpressionAttributeValues={
                ":s": {"N": str(self.votes)},
                ":c": {"N": str(self.comments)},
                ":t": {"N": str(time.time())},
                ":p": {"BOOL": self.posted}
            }
        )

    def _is_in_db(self):
        client = boto3.client('dynamodb')
        response = client.get_item(
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}}
        )
        # This is the condition for 'presence' in the database
        in_db = 'Item' in response
        if in_db:
            self.posted = response['Item']['posted']['BOOL']
        return in_db

    def _image_was_posted(self):
        client = boto3.client('dynamodb')
        response = client.get_item(
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}}
        )
        # This is the condition for 'presence' in the database
        if 'Item' in response and 'posted' in response['Item']:
            return response['Item']['posted']['BOOL'] # god I hate dynamodb
        return False

    def ensure_image_downloaded(self):
        current_dir = get_current_dir()
        filename = "{dir}/{sub}/images/{f}".format(
            dir=current_dir, 
            sub=self.subreddit, 
            f=self.id)
        try:
            f = open(filename, 'rb')
            return True
        except FileNotFoundError:
            # download image from s3
            client = boto3.client('s3')
            current_dir = get_current_dir()
            try:
                client.download_file(
                    Bucket=CURRENT_BUCKET,
                    # object name (subreddit/id - eg memes/adef1223f47bc95bc95 )
                    Key="{}/{}".format(self.subreddit, self.id),
                    Filename=filename
                )
                print("downloaded image " + self.id)
                return True
            except:
                print('not in s3')
                return False

    # These requirements will change over time - see analyze.py file for testing 
    def should_post_to_instagram(self):
        if self._image_was_posted():
            print('image was previously posted')
            return False
        time_since_posted = time.time() - self.created # both utc
        print(self.title, time_since_posted)
        ups_ratio = self.votes / time_since_posted #  ups = upvotes per second
        print(ups_ratio)

        # filters out all posts with banned words for a specific
        for word in secrets.BANNED_PAGE_WORDS[self.subreddit]:
            for post_word in self.title.split(" "):
                if word == post_word.lower():
                    return False

        client = boto3.client('dynamodb')
        response = client.get_item( # switch to batch get item to optimize DB calls
            TableName=CURRENT_TABLE,
            Key={'id': {'S': self.id}}
        )

        if 'Item' not in response:
            n = 1
        else:
            # Get size of engagement log
            n = len(response['Item']['engagement']['M']['timestamps']['L'])

        # first X hours of seeing this post (if n < X)
        if n < len(secrets.VALUES_FOR_SUBREDDIT[self.subreddit]):
            return ups_ratio > secrets.VALUES_FOR_SUBREDDIT[self.subreddit][n-1]
        else:
            return False


    def post_to_instagram(self):
        account_name = secrets.ACCOUNT_NAME_FOR_SUBREDDIT[self.subreddit]
        account_password = secrets.ACCOUNT_PASSWORD_FOR_SUBREDDIT[self.subreddit]

        client = boto3.client('dynamodb')
        account_state = client.get_item(
            TableName='account-state',
            Key={'account': {'S': account_name}},
        )
        # We should post to the main instagram if there are fewer than 2 posts in the last 24 hours
        # Post to my story otherwise 
        post_to_main_insta = int(account_state['Item']['posts_today']['N']) < 2

        print("posting image " + self.id)
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

            if post_to_main_insta:
                requests.post(secrets.API_URL + '/instant', files=form_file, data=payload)
            else:
                requests.post(secrets.API_URL + '/instant', files=form_file, data=payload)
                # requests.post(secrets.API_URL + '/story', files=form_file, data=payload)
                print("would post this to story")

        # update account state DDB
        try:
            client.update_item(
                TableName='account-state',
                Key={'account': {'S': account_name}},
                UpdateExpression= "ADD posts_today :v",
                ExpressionAttributeValues={
                    ":v": {"N": str(1)},
                }
            )
        except Exception as e: 
            print(e)
        self.posted = True # update the entry in Dynamo

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
        print(self.id, self.posted)
        client.put_item(
            TableName=CURRENT_TABLE,
            Item={
                'community': {'S': self.subreddit},
                'community_size': {'N': str(self.subreddit_size)},
                'content_source': {'S': 'reddit.com'},
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
                },
                'posted': {'BOOL': self.posted}
            } 
        )
