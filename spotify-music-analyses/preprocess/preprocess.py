import os
import logging
import argparse
import csv
from pathlib import Path
import glob

import pandas as pd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from utils.params import Client_ID, Client_Secret


# global variable
columns = {
    1: 'title',
    2: 'rank',
    3: 'date',
    4: 'artist',
    5: 'url',
    6: 'region',
    7: 'chart',
    8: 'streams', 
    9: 'year',
    10: 'month',
    11: 'danceability', 
    12: 'energy',
    13: 'key', 
    14: 'loudness', 
    15: 'mode', 
    16: 'speechiness', 
    17: 'acousticness', 
    18: 'instrumentalness', 
    19: 'liveness', 
    20: 'valence', 
    21: 'tempo', 
    22: 'duration_ms', 
    23: 'time_signature',
    24: 'null' 
}


def processing(line):
        url = line[5]
        d = sp.audio_features(url)[0]
        del d['type'], d['id'], d['uri'], d['track_href'], d['analysis_url']
        line += list(d.values())
        s = ''.join(str(e) + ', ' for e in line)
        return s

def create_feature_data(load_path, save_path):
    with open(load_path,'r') as load_file, open(save_path,'w') as save_file:
        datareader = csv.reader(load_file)
        next(datareader)
        for line in datareader:
            save_line = processing(line)
            save_file.write(save_line)
            save_file.write('\n')

    # remove intermidiate files
    p = Path(load_path)
    p.unlink()
    return None

def concat_feature(
    data_dir: str,
    save_dir: str,
    suffix: str,
    year: int
    ):
    data_dir = data_dir + suffix + '/' + str(year) + '/'
    save_dir = save_dir + suffix + '/' + str(year) + '/'
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    os.makedirs(os.path.dirname(save_dir), exist_ok=True)

    files = os.listdir(data_dir)
    file_list = [filename for filename in files if filename.split('.')[1]=='csv']

    for filename in file_list:
        load_path = data_dir + filename
        save_path = save_dir + filename
        if not os.path.exists(save_path):
            create_feature_data(load_path, save_path)
            logger.info('saved to {}'.format(save_path))

    # concatenate all intermidiate files
    def read_csv(file):
        df = pd.read_csv(file, index_col=0, header=None, on_bad_lines='skip').rename(columns=columns)
        df = df.drop(columns='null')
        return df

    files = glob.glob(save_dir+'*.csv')
    df_all = pd.concat(map(read_csv, files), ignore_index=True)
    save_path = save_dir+'all.csv'

    # remove intermidiate files
    p = Path(save_dir)
    for x in p.iterdir():
        if x.is_file():
            x.unlink()

    def str_to_datetime(s):
        if len(s.split('-'))==3:
            return pd.to_datetime(s)

    df_all['date'] = df_all.date.apply(str_to_datetime)
    df_all.to_csv(save_path, index=False)

    return None

if __name__ == '__main__':
    data_dir = 'data/sep/'
    save_dir = 'data/con/'

    os.environ['SPOTIPY_CLIENT_ID'] = Client_ID
    os.environ['SPOTIPY_CLIENT_SECRET'] = Client_Secret

    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)

    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    parser = argparse.ArgumentParser(description='Merge datasets.')
    parser.add_argument('suffix', type=str, help='save file suffix')
    parser.add_argument('--data_dir', default=data_dir, help='load path')
    parser.add_argument('--save_dir', default=save_dir, help='output directry')
    parser.add_argument('--year', default=2021, help='year')
    args = parser.parse_args()

    concat_feature(
        data_dir = args.data_dir,
        save_dir = args.save_dir,
        suffix = args.suffix,
        year = args.year
    )


