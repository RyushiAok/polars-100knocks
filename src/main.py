import os
from os.path import dirname, join

import polars as pl
from dotenv import dotenv_values

from src.utils.dataset import load_100knock_data

config = {
    **dotenv_values(join(dirname(__file__), "../.envs.example")),
    **dotenv_values(join(dirname(__file__), "../.env.local")),
}

if config["DATASET_DIR"] is None:
    print("環境変数が設定されていません。")
    exit()

DATASET_DIR: str = (
    config["DATASET_DIR"] if os.path.isabs(config["DATASET_DIR"]) else join(dirname(__file__), "../", config["DATASET_DIR"])
)

dataset = load_100knock_data(DATASET_DIR)


def p_001() -> None:
    """
    レシート明細データ（df_receipt）から全項目の先頭10件を表示し、どのようなデータを保有しているか目視で確認せよ。
    """
    print(dataset.df_receipt.head(10))


def p_002() -> None:
    """
    レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示せよ。
    """
    print(dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).head(10))


def p_003() -> None:
    """
    レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示せよ。ただし、sales_ymdsales_dateに項目名を変更しながら抽出すること。
    """
    print(dataset.df_receipt.select([pl.col("sales_ymd").alias("sales_date"), "customer_id", "product_cd", "amount"]).head(10))


def p_004() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    """
    print(
        dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
            pl.col("customer_id") == "CS018205000001"
        )
    )


p_004()
