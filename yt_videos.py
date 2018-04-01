import pandas as pd
import requests
import pyjq
from tabulate import tabulate
import csv

apikey = 'AIzaSyB0_VkjAlTnlGM-fsKdgbG60sctGInqgAI'
channelid = 'UCXazgXDIYyWH-yXLAkcrFxw'
nvideos = '50'
videoid_url = 'https://www.googleapis.com/youtube/v3/search?key=' + apikey + '&channelId=' + channelid +'&part=snippet,id&order=date&maxResults=' + nvideos


videoids = pyjq.all('.items[]|.id|.videoId', url=videoid_url)


videoids_length = len(videoids)
nextPageToken = ''
commentid_list= []

for i in range(videoids_length):

    videoid = videoids[i]
    for n in range(10):
        commentid_url = 'https://www.googleapis.com/youtube/v3/commentThreads?key=' + str(apikey) + '&textFormat=plainText&part=snippet&videoId=' + str(videoid) + '&maxResults=100' + '&pageToken=' + str(nextPageToken)
        nextPageToken = pyjq.all('.nextPageToken', url=commentid_url)
        commentids = pyjq.all('.items[]|.snippet|.topLevelComment|.snippet|.authorChannelId|.value', url=commentid_url)
        commentid_list += commentids
        print(str(i) + ' video_id: ' + str(videoid) + ' | pageToken: ' + str(n))
        

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
    userid = filtered['commenterid'].iloc[i]
    userid_url = 'https://www.googleapis.com/youtube/v3/channels?key=' + apikey + '&id=' + userid + '&part=snippet'
    country_list = pyjq.all('.items[]|.snippet|.country', url=userid_url)
    countries_list += country_list
    print(i)
    
country_df = pd.DataFrame(countries_list)
country_df.columns = ['country']

comenters_countries_df = filtered.join(country_df, how='inner')
print(comenters_countries_df)
# print(tabulate(comenters_countries_df, headers='keys', tablefmt='psql'))
with open("/Users/KimiaIMB1/scripts/geos.csv", "w") as file:
      file.write(str(comenters_countries_df))

