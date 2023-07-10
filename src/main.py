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


def p_005() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 売上金額（amount）が1,000以上
    """
    print(
        dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
            (pl.col("customer_id") == "CS018205000001") & (pl.col("amount") >= 1000)
        )
    )


def p_006() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上数量（quantity）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 売上金額（amount）が1,000以上または売上数量（quantity）が5以上
    """
    print(
        dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "quantity", "amount"]).filter(
            (pl.col("customer_id") == "CS018205000001") & ((pl.col("amount") >= 1000) | (pl.col("quantity") >= 5))
        )
    )


def p_007() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 売上金額（amount）が1,000以上2,000以下
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
        (pl.col("customer_id") == "CS018205000001") & (pl.col("amount").is_between(1000, 2000))
    )
    print(res)


def p_008() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 商品コード（product_cd）が"P071401019"以外
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
        (pl.col("customer_id") == "CS018205000001") & (pl.col("product_cd") != "P071401019")
    )
    print(res)


def p_009() -> None:
    """
    以下の処理において、出力結果を変えずにORをANDに書き換えよ。

    `df_store.query('not(prefecture_cd == "13" | floor_area > 900)')`
    """
    res = dataset.df_store.filter((pl.col("prefecture_cd") != "13") & (pl.col("floor_area") <= 900))
    print(res)


def p_010() -> None:
    """店舗データ（df_store）から、店舗コード（store_cd）が"S14"で始まるものだけ全項目抽出し、10件表示せよ。"""
    res = dataset.df_store.filter(pl.col("store_cd").str.starts_with("s14")).head(10)
    print(res)


def p_011() -> None:
    """顧客データ（df_customer）から顧客ID（customer_id）の末尾が1のものだけ全項目抽出し、10件表示せよ。"""
    res = dataset.df_customer.filter(pl.col("customer_id").str.ends_with("1")).head(10)
    print(res)


def p_012() -> None:
    """店舗データ（df_store）から、住所 (address) に"横浜市"が含まれるものだけ全項目表示せよ。"""
    res = dataset.df_store.filter(pl.col("address").str.contains("横浜市"))
    print(res)


# 正規表現


def p_013() -> None:
    """顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まるデータを全項目抽出し、10件表示せよ。"""
    res = dataset.df_customer.filter(pl.col("status_cd").str.contains(r"^[A-F]")).head(10)
    print(res)


def p_014() -> None:
    """顧客データ（df_customer）から、ステータスコード（status_cd）の末尾が数字の1〜9で終わるデータを全項目抽出し、10件表示せよ。"""
    res = dataset.df_customer.filter(pl.col("status_cd").str.contains(r"[1-9]$")).head(10)
    print(res)


def p_015() -> None:
    """顧客データ（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まり、末尾が数字の1〜9で終わるデータを全項目抽出し、10件表示せよ。"""
    res = dataset.df_customer.filter(pl.col("status_cd").str.contains(r"^[A-F].*[1-9]$")).head(10)
    print(res)


def p_016() -> None:
    """店舗データ（df_store）から、電話番号（tel_no）が3桁-3桁-4桁のデータを全項目表示せよ。"""
    res = dataset.df_store.filter(pl.col("tel_no").str.contains(r"^\d{3}-\d{3}-\d{4}"))
    print(res)


def p_017() -> None:
    """顧客データ（df_customer）を生年月日（birth_day）で高齢順にソートし、先頭から全項目を10件表示せよ。"""
    res = dataset.df_customer.sort("birth_day").select(["birth_day"]).head(100)
    print(res)


def p_018() -> None:
    """顧客データ（df_customer）を生年月日（birth_day）で若い順にソートし、先頭から全項目を10件表示せよ。"""
    res = dataset.df_customer.sort("birth_day", descending=True).head(10)
    print(res)


def p_019() -> None:
    """
    レシート明細データ（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭から10件表示せよ。
    項目は顧客ID（customer_id）、売上金額（amount）、付与したランクを表示させること。
    なお、売上金額（amount）が等しい場合は同一順位を付与するものとする。
    """
    res = (
        dataset.df_receipt.select(
            ["customer_id", "amount", pl.col("amount").rank(method="max", descending=True).alias("ranking")]
        )
        .sort("ranking")
        .head(10)
    )
    print(res)


def p_020() -> None:
    """
    レシート明細データ（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭から10件表示せよ。
    項目は顧客ID（customer_id）、売上金額（amount）、付与したランクを表示させること。
    なお、売上金額（amount）が等しい場合でも別順位を付与すること。
    """
    data = (
        dataset.df_receipt.select(
            [
                pl.col("customer_id"),
                pl.col("amount"),
                pl.col("amount").rank(method="random", descending=True).alias("ranking"),
            ]
        )
        .sort("ranking")
        .head(10)
    )

    print(data)


p_019()
