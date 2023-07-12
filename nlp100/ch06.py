import os
from os.path import dirname, join
from typing import Any, Mapping

import polars as pl
from dotenv import dotenv_values
from sklearn.model_selection import train_test_split

config = {
    **dotenv_values(join(dirname(__file__), "../.env.example")),
    **dotenv_values(join(dirname(__file__), "../.env.local")),
}

if config["NLP100_DATASET_NEWS"] is None:
    print("環境変数が設定されていません。")
    exit()

NLP100_DATASET_NEWS: str = (
    config["NLP100_DATASET_NEWS"]
    if os.path.isabs(config["NLP100_DATASET_NEWS"])
    else join(dirname(__file__), "../", config["NLP100_DATASET_NEWS"])
)

dtypes: Mapping[str, Any] = {
    "id": str,
    "title": str,
    "url": str,
    "publisher": str,
    "category": str,
    "story": str,
    "hostname": str,
    "timestamp": str,
}


def p_050() -> None:
    df_news_corpus = pl.read_csv(
        join(NLP100_DATASET_NEWS, "./newsCorpora.csv"),
        dtypes=dtypes,
        separator="\t",
        has_header=False,
        ignore_errors=True,
    ).with_columns(pl.from_epoch("timestamp", time_unit="ms"))

    data = df_news_corpus.filter(
        pl.col("publisher").is_in(
            [
                "Reuters",
                "Huffington Post",
                "Businessweek",
                "Contactmusic.com",
                "Daily Mail",
            ]
        )
    )

    train: pl.DataFrame
    valid: pl.DataFrame
    test: pl.DataFrame

    train, valid_test = train_test_split(
        data,
        test_size=0.2,
        shuffle=True,
        random_state=123,
        stratify=data["category"],  # 指定したカラムの構成比が分割後の各データで等しくなるように分割する
    )

    valid, test = train_test_split(
        valid_test,
        test_size=0.5,
        shuffle=True,
        random_state=123,
        stratify=valid_test["category"],
    )

    print(
        train.groupby("category")
        .agg(count=pl.count())
        .sort("count", descending=True)
    )
    print(
        valid.groupby("category")
        .agg(count=pl.count())
        .sort("count", descending=True)
    )
    print(
        test.groupby("category")
        .agg(count=pl.count())
        .sort("count", descending=True)
    )

    train.write_csv(join(NLP100_DATASET_NEWS, "./ch06/train.csv"))
    valid.write_csv(join(NLP100_DATASET_NEWS, "./ch06/valid.csv"))
    test.write_csv(join(NLP100_DATASET_NEWS, "./ch06/test.csv"))
