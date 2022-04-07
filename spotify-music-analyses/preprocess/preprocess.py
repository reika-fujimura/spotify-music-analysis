import os
import logging
import argparse
import csv
from pathlib import Path

import pandas as pd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from utils.params import Client_ID, Client_Secret, SPOTIPY_REDIRECT_URI


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
        count = 0
        for line in datareader:
            save_line = processing(line)
            save_file.write(save_line)
            save_file.write('\n')
            count += 1
            if count%500 == 0:
                logger.info(count)

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
        load_path = data_dir + filename.split('/')[-1]
        save_path = save_dir + filename.split('/')[-1]
        create_feature_data(load_path, save_path)
        logger.info('saved to {}'.format(save_path))
    
    p = Path(data_dir)
    p.unlink

    return None

if __name__ == '__main__':
    data_dir = 'data/sep/'
    save_dir = 'data/con/'

    os.environ['SPOTIPY_CLIENT_ID'] = Client_ID
    os.environ['SPOTIPY_CLIENT_SECRET'] = Client_Secret
    os.environ['SPOTIPY_REDIRECT_URI'] = SPOTIPY_REDIRECT_URI

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


