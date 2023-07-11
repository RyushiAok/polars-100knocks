import os
from os.path import dirname, join

from dotenv import dotenv_values

import src.knocks as knocks
from src.utils.dataset import load_100knock_data

config = {
    **dotenv_values(join(dirname(__file__), "../.env.example")),
    **dotenv_values(join(dirname(__file__), "../.env.local")),
}

if config["DATASET_DIR"] is None:
    print("環境変数が設定されていません。")
    exit()

DATASET_DIR: str = (
    config["DATASET_DIR"]
    if os.path.isabs(config["DATASET_DIR"])
    else join(dirname(__file__), "../", config["DATASET_DIR"])
)

dataset = load_100knock_data(DATASET_DIR)

knocks.p_050(dataset)
