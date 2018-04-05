import os
import sys

import pandas as pd
import requests
import pyjq
from tabulate import tabulate
import csv
import logging
import time

start_time = time.time()


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/Users/KimiaIMB1/scripts/myapp.txt')
logger = logging.getLogger(__name__)

# Get the api key from the environment: env GOOGLE_API_KEY=... python yt_videos.py
#apikey = os.environ['GOOGLE_API_KEY']
apikey = 'AIzaSyB0_VkjAlTnlGM-fsKdgbG60sctGInqgAI'
# Get the channel from the first argument
#channel_id = sys.argv[1]
channelid = 'UCXazgXDIYyWH-yXLAkcrFxw'
nvideos = '50'
videoid_url = 'https://www.googleapis.com/youtube/v3/search?key=' + apikey + '&channelId=' + channelid +'&part=snippet,id&order=date&maxResults=' + nvideos


videoids = pyjq.all('.items[]|.id|.videoId', url=videoid_url)


nextPageToken = ''
commentid_list= []

# videoids is a list so we can do a loop over it directly
for videoid in videoids:
    try: 
        for n in range(10):
            try:
   	        commentid_url = 'https://www.googleapis.com/youtube/v3/commentThreads?key=' + str(apikey) + '&textFormat=plainText&part=snippet&videoId=' + str(videoid) + '&maxResults=100' + '&pageToken=' + str(nextPageToken)
                nextPageToken = pyjq.all('.nextPageToken', url=commentid_url)
                commentids = pyjq.all('.items[]|.snippet|.topLevelComment|.snippet|.authorChannelId|.value', url=commentid_url)
                commentid_list += commentids
                print('video_id: {} | pageToken: {}'.format(videoid, n))		      
            except Exception as ex:
                logger.error('Error on video %s page %s: %s', videoid, n, ex, exc_info=True)
  
    except Exception as ex:
        logger.error('Error on video %s: %s', videoid, ex, exc_info=True)
   

commentid_df = pd.DataFrame(commentid_list)
commentid_df.columns = ['commenterid']
commentid_df['count'] = commentid_df.groupby('commenterid')['commenterid'].transform('count')
comment_clean_df = commentid_df.drop_duplicates(subset=['commenterid', 'count'], keep='first')

com_clean_df = comment_clean_df.reset_index(drop=True)

filter1 = com_clean_df[(com_clean_df['count'] > 2)]
filtered = filter1.reset_index(drop=True)

filtered_length = len(filtered)


countries_list=[]

for i in range(filtered_length):
    try: 
        userid = filtered['commenterid'].iloc[i]
        userid_url = 'https://www.googleapis.com/youtube/v3/channels?key=' + apikey + '&id=' + userid + '&part=snippet'
        country_list = pyjq.all('.items[]|.snippet|.country', url=userid_url)
        countries_list += country_list
        print(i)

    except Exception as ex:
        logger.error(ex, exc_info=True)

country_df = pd.DataFrame(countries_list)
country_df.columns = ['country']

comenters_countries_df = filtered.join(country_df, how='inner')
channel_countries = comenters_countries_df[comenters_countries_df.country.str.contains("None") == False]


with pd.option_context('display.max_rows', None, 'display.max_columns', 3):
    print(channel_countries.to_string(index=False))

channel_countries.to_csv("/Users/KimiaIMB1/scripts/geos.csv", sep='\t')

print("--- %s minutes ---" % ((time.time() - start_time)/60))
