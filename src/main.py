import os
from os.path import dirname, join

import polars as pl
from dotenv import dotenv_values

from src.utils.dataset import load_100knock_data

config = {
    **dotenv_values(join(dirname(__file__), "../.env.example")),
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
    res = dataset.df_receipt.head(10)
    print(res)


def p_002() -> None:
    """
    レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示せよ。
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).head(10)
    print(res)


def p_003() -> None:
    """
    レシート明細データ（df_receipt）から売上年月日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示せよ。ただし、sales_ymdsales_dateに項目名を変更しながら抽出すること。
    """
    res = dataset.df_receipt.select([pl.col("sales_ymd").alias("sales_date"), "customer_id", "product_cd", "amount"]).head(10)
    print(res)


def p_004() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
        pl.col("customer_id") == "CS018205000001"
    )
    print(res)


def p_005() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 売上金額（amount）が1,000以上
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).filter(
        (pl.col("customer_id") == "CS018205000001") & (pl.col("amount") >= 1000)
    )
    print(res)


def p_006() -> None:
    """
    レシート明細データ（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、売上数量（quantity）、売上金額（amount）の順に列を指定し、以下の全ての条件を満たすデータを抽出せよ。

    - 顧客ID（customer_id）が"CS018205000001"
    - 売上金額（amount）が1,000以上または売上数量（quantity）が5以上
    """
    res = dataset.df_receipt.select(["sales_ymd", "customer_id", "product_cd", "quantity", "amount"]).filter(
        (pl.col("customer_id") == "CS018205000001") & ((pl.col("amount") >= 1000) | (pl.col("quantity") >= 5))
    )
    print(res)


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


def p_021() -> None:
    """レシート明細データ（df_receipt）に対し、件数をカウントせよ。"""
    res = len(dataset.df_receipt)
    print(res)


def p_022() -> None:
    """レシート明細データ（df_receipt）の顧客ID（customer_id）に対し、ユニーク件数をカウントせよ。"""
    res = len(dataset.df_receipt.select(pl.col("customer_id").n_unique()))
    print(res)


def p_023() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）と売上数量（quantity）を合計せよ。"""
    res = dataset.df_receipt.groupby("store_cd").agg([pl.col("amount").sum(), pl.col("quantity").sum()])
    print(res)


def p_024() -> None:
    """レシート明細データ（df_receipt）に対し、顧客ID（customer_id）ごとに最も新しい売上年月日（sales_ymd）を求め、10件表示せよ。"""
    res = dataset.df_receipt.groupby("customer_id").agg(pl.col("sales_ymd").max()).head(10)
    print(res)


def p_025() -> None:
    """レシート明細データ（df_receipt）に対し、顧客ID（customer_id）ごとに最も古い売上年月日（sales_ymd）を求め、10件表示せよ。"""
    res = dataset.df_receipt.groupby("customer_id").agg(pl.col("sales_yml").min()).head(10)
    print(res)


def p_026() -> None:
    """レシート明細データ（df_receipt）に対し、顧客ID（customer_id）ごとに最も新しい売上年月日（sales_ymd）と古い売上年月日を求め、両者が異なるデータを10件表示せよ。"""
    res = (
        dataset.df_receipt.groupby("customer_id")
        .agg([pl.col("sales_ymd").min().alias("sales_ymd_min"), pl.col("sales_ymd").max().alias("sales_ymd_max")])
        .filter(pl.col("sales_ymd_min") != pl.col("sales_ymd_max"))
        .sort("customer_id")
        .head(10)
    )
    print(res)


def p_027() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の平均を計算し、降順でTOP5を表示せよ。"""
    res = (
        dataset.df_receipt.groupby("store_cd")
        .agg(pl.col("amount").mean().alias("amount_mean"))
        .sort("amount_mean", descending=True)
        .head(5)
    )
    print(res)


def p_028() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の中央値を計算し、降順でTOP5を表示せよ。"""
    res = (
        dataset.df_receipt.groupby("store_cd")
        .agg(pl.col("amount").median().alias("amount_median"))
        .sort("amount_median", descending=True)
        .head(5)
    )
    print(res)


def p_029() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに商品コード（product_cd）の最頻値を求め、10件表示させよ。"""
    res = dataset.df_receipt.groupby("store_cd").agg(pl.col("product_cd").mode()).select(["store_cd", pl.col("product_cd").arr])
    print(res)


def p_030() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の分散を計算し、降順で5件表示せよ。"""
    res = dataset.df_receipt.groupby("store_cd").agg(pl.col("amount").var()).sort("amount", descending=True).head(5)
    print(res)


def p_031() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の標準偏差を計算し、降順で5件表示せよ。"""
    res = dataset.df_receipt.groupby("store_cd").agg(pl.col("amount").std()).sort("amount", descending=True).head(5)
    print(res)


def p_032() -> None:
    """レシート明細データ（df_receipt）の売上金額（amount）について、25%刻みでパーセンタイル値を求めよ。"""
    res = dataset.df_receipt.select([pl.col("amount").quantile(i).alias(f"q_{i}") for i in [0, 0.25, 0.5, 0.75, 1]])
    print(res)


def p_033() -> None:
    """レシート明細データ（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の平均を計算し、330以上のものを抽出せよ。"""
    res = dataset.df_receipt.groupby("store_cd").agg(pl.col("amount").mean()).filter(pl.col("amount") >= 330).sort("store_cd")
    print(res)


def p_034() -> None:
    """
    レシート明細データ（df_receipt）に対し、顧客ID（customer_id）ごとに売上金額（amount）を合計して全顧客の平均を求めよ。
    ただし、顧客IDが"Z"から始まるものは非会員を表すため、除外して計算すること
    """
    res = (
        dataset.df_receipt.filter(pl.col("customer_id").str.starts_with("Z").is_not())
        .groupby("customer_id")
        .agg(pl.col("amount").sum())
        .select(pl.col("amount").mean())
    )
    print(res)


def p_035() -> None:
    """
    レシート明細データ（df_receipt）に対し、顧客ID（customer_id）ごとに売上金額（amount）を合計して全顧客の平均を求め、
    平均以上に買い物をしている顧客を抽出し、10件表示せよ。
    ただし、顧客IDが"Z"から始まるものは非会員を表すため、除外して計算すること。
    """
    res = (
        dataset.df_receipt.filter(pl.col("customer_id").str.starts_with("Z").is_not())
        .groupby("customer_id")
        .agg(pl.col("amount").sum())
        .with_columns(pl.col("amount").mean().alias("avg_amount"))
        .filter(pl.col("amount") >= pl.col("avg_amount"))
        .head(10)
    )
    print(res)


def p_036() -> None:
    """レシート明細データ（df_receipt）と店舗データ（df_store）を内部結合し、レシート明細データの全項目と店舗データの店舗名（store_name）を10件表示せよ。"""
    res = dataset.df_receipt.join(
        dataset.df_store.select(["store_cd", "store_name"]),
        on="store_cd",
        how="inner",
    ).head(10)
    print(res)


def p_037() -> None:
    """
    商品データ（df_product）とカテゴリデータ（df_category）を内部結合し、
    商品データの全項目とカテゴリデータのカテゴリ小区分名（category_small_name）を10件表示せよ
    """
    res = dataset.df_product.join(
        dataset.df_category.select(["category_small_cd", "category_small_name"]),
        on="category_small_cd",
        how="inner",
    ).head(10)
    print(res)


def p_038() -> None:
    """
    顧客データ（df_customer）とレシート明細データ（df_receipt）から、顧客ごとの売上金額合計を求め、10件表示せよ。
    ただし、売上実績がない顧客については売上金額を0として表示させること。
    また、顧客は性別コード（gender_cd）が女性（1）であるものを対象とし、非会員（顧客IDが"Z"から始まるもの）は除外すること。
    """
    res = (
        dataset.df_customer.filter((pl.col("gender_cd") == "1") & (pl.col("customer_id").str.starts_with("Z").is_not()))
        .join(dataset.df_receipt, on="customer_id", how="left")
        .groupby("customer_id")
        .agg(pl.col("amount").sum().fill_null(0))
        .head(10)
    )
    print(res)


def p_039() -> None:
    """
    レシート明細データ（df_receipt）から、売上日数の多い顧客の上位20件を抽出したデータと、
    売上金額合計の多い顧客の上位20件を抽出したデータをそれぞれ作成し、さらにその2つを完全外部結合せよ。
    ただし、非会員（顧客IDが"Z"から始まるもの）は除外すること。
    """
    df_member_data = dataset.df_receipt.filter(pl.col("customer_id").str.starts_with("Z").is_not())
    df_cnt = (
        df_member_data.groupby("customer_id")
        .agg(pl.col("sales_ymd").n_unique().alias("cnt_sales_ymd"))
        .sort("cnt_sales_ymd", descending=True)
        .head(20)
    )
    df_sum = (
        df_member_data.groupby("customer_id")
        .agg(pl.col("amount").sum().alias("sum_amount"))
        .sort("sum_amount", descending=True)
        .head(20)
    )
    res = df_cnt.join(df_sum, on="customer_id", how="outer")
    print(res)


def p_040() -> None:
    """全ての店舗と全ての商品を組み合わせたデータを作成したい。店舗データ（df_store）と商品データ（df_product）を直積し、件数を計算せよ。"""
    res = (
        dataset.df_store.with_columns(pl.lit(0).alias("key"))
        .join(dataset.df_product.with_columns(pl.lit(0).alias("key")), on="key", how="inner")
        .shape
    )
    print(res)


p_040()
