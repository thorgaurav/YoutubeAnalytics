import requests
from constants import YOUTUBE_API_KEY 
import json
from pprint import pprint
from confluent_kafka import Producer


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers':'localhost:9092',
        'client.id':'myEC2Instance'
    }
    producer = Producer(producer_config)
    video_id = '3_tC83fCHzQ'
    api_key = YOUTUBE_API_KEY
    # url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={api_key}"
    # response = requests.get(url)
    # print(response.text)

    response = requests.get('https://www.googleapis.com/youtube/v3/videos', {
        'key':api_key,
        'id':video_id,
        'part':'snippet, statistics, status' 
    })
    # print(response.text)
    response = json.loads(response.text)['items']
    for video in response:
        # print(video)
        video_res = {
            'title':video['snippet']['title'],
            'likes':int(video['statistics'].get('likeCount', 0)),
            'comments':int(video['statistics'].get('commentCount', 0)),
            'views':int(video['statistics'].get('viewCount', 0)),
            'favorites':int(video['statistics'].get('favoriteCount', 0)),
            'thumbnail':video['snippet']['thumbnails']['default']['url']
        }
        print(pprint(video_res))

        producer.produce('youtube_videos', json.dumps(video_res).encode('utf-8'))
        producer.flush()