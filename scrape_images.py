#pypi
import click
#local modules
from reddit_scraper import RedditScraper

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
