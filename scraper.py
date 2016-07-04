
# coding: utf-8

# In[1]:

from __future__ import print_function
from twisted.internet import reactor
import json
import configparser
import scrapy
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
import sys

from scrapy.crawler import CrawlerRunner
import urllib
from urlparse import urlparse, parse_qs
from scrapy.crawler import CrawlerProcess

cfg = configparser.ConfigParser()
cfg.read('apikeys.config')
apikey = cfg['lastfm']['apikey']

filename = sys.argv[1]
start = int(sys.argv[2])
count = int(sys.argv[3])


# In[2]:

recent_tracks = "http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks" + \
    "&user={username}&api_key={apikey}&limit={limit}&page={page}&format=json"
user_info = "http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user={username}" + \
    "&api_key={apikey}&format=json"

remove_this = "http://www.last.fm/music/"

# In[4]:

with open("data/users.txt") as f:
  usernames = [line.split(',')[1] for line in f]
starters = [user_info.format(username=x,apikey=apikey,limit=1000,page=1) for x in usernames]



# In[5]:

def userinfo_to_tracklists(resp):
    userinfo = json.loads(resp)
    uname = userinfo["user"]["name"]
    pc = int(userinfo["user"]["playcount"])
    num_pages = int(pc / 1000) + 1
    return (recent_tracks.format(username=uname,apikey=apikey,limit=1000,page=x) \
        for x in range(1,num_pages+1))


# In[8]:


class LastFM(scrapy.Spider):
  name = 'lastfm'
  # start_urls = [user_info.format(username='kaytwo',apikey=apikey),]
  start_urls =starters[start:start+count] 
  

  def parse(self, response):

    response_str = response.body
    for item in userinfo_to_tracklists(response_str):
      yield scrapy.Request(item,self.dump_response)
          
  def dump_response(self, response):
    # return json.loads(response.body)
    resp = json.loads(response.body)
    
    try:
      username = resp["recenttracks"]['@attr']['user']
      page = resp["recenttracks"]["@attr"]["page"]
      icare = resp["recenttracks"]["track"]
    except KeyError as e:
      self.logger.error("missing key: {} url: {} resp: {}".format(str(e),response.url, response.body[:100]))
      return

    # return json.loads(response.body)

    def extract_info(item):
      for x in item:
        url = x['url'].replace(remove_this,"",1)
        ts = x.get("date",{}).get("uts","")
        yield {"url":url,"ts":ts}

    return {'user':username,"page":page,'v': list(extract_info(icare))}
    # return json.loads(response.body)


# In[ ]:

settings = get_project_settings()
settings.set('FEED_URI',"{}_{}_{}.json".format(filename,start,start+count),'cmdline')
settings.set("CONCURRENT_REQUESTS",20,"cmdline")
settings.set("CONCURRENT_REQUESTS_PER_DOMAIN",20,"cmdline")
settings.set("LOG_LEVEL","INFO","cmdline")

process = CrawlerProcess(settings)
process.crawl(LastFM)
process.start() # the script will block here until the crawling is finished


