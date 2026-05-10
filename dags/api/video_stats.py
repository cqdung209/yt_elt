import requests
import json
from airflow.operators.python import get_current_context
#import os
#from dotenv import load_dotenv
#load_dotenv(dotenv_path='./.env')
from pathlib import Path
import datetime
from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
max_results = 50

@task
def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}'

        response = requests.get(url)
        #print (response)
        response.raise_for_status()
        data = response.json()
        #print (json.dumps(data,indent=4))

        channel_items = data["items"][0]
        #print(channel_items)
        channel_playlistid = channel_items["contentDetails"]["relatedPlaylists"]['uploads']
        #print(channel_playlistid)
        return channel_playlistid
    except requests.exceptions.RequestException as e:
        raise e

@task    
def get_video_ids(playlist_id):
    video_ids = []
    page_token = None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}'
    
    try:
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            #print (json.dumps(data,indent=4))

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)
            
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return video_ids
            
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def extract_video_data(video_ids):
    extract_data = []
    
    def batch_list(video_ids, max_results):
        for i in range(0, len(video_ids), max_results):
            yield video_ids[i:i + max_results]
    
    try:
        for batch in batch_list(video_ids, max_results):
            video_id_string = ",".join(batch)
            batch_url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_id_string}&key={API_KEY}'
            response = requests.get(batch_url)
            response.raise_for_status()
            data = response.json()
            #print (json.dumps(data,indent=4))

            for item in data.get("items", []):
                video_data = {
                    "videoId": item["id"],
                    "title": item["snippet"]["title"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "duration": item["contentDetails"]["duration"],
                    "viewCount": item["statistics"].get("viewCount", 0),
                    "likeCount": item["statistics"].get("likeCount", 0),
                    "commentCount": item["statistics"].get("commentCount", 0)
                }
                extract_data.append(video_data)
        return extract_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extract_data):   

    context = get_current_context()
    ds = context["ds"]
    file_date = ds.replace("-", "")
    file_path = Path("/opt/airflow/data") / f"video_data_{file_date}.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(extract_data, f, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)