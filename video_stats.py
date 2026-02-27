import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='./.env')
API_KEY = os.getenv("API_KEY")

CHANNEL_HANDLE = 'MrBeast'

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
        print(channel_playlistid)
        return channel_playlistid
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__ == '__main__':
    get_playlist_id()