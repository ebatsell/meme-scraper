from setuptools import setup

setup(
    name='redditScraper',
    version='1.0',
    install_requires=[
        'boto3',
        'click',
        'requests',
        'wget'
    ],
)

