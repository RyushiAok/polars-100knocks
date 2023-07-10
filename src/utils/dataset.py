from dataclasses import dataclass
from os.path import join
from typing import Any, Mapping

import polars as pl

dtypes: Mapping[str, Any] = {
    "customer_id": str,
    "gender_cd": str,
    "postal_cd": str,
    "application_store_cd": str,
    "status_cd": str,
    "category_major_cd": str,
    "category_medium_cd": str,
    "category_small_cd": str,
    "product_cd": str,
    "store_cd": str,
    "prefecture_cd": str,
    "tel_no": str,
    "postal_cd": str,
    "street": str,
    "application_date": str,
    "birth_day": pl.Date,
}


@dataclass
class Dataset:
    df_customer: pl.DataFrame
    df_category: pl.DataFrame
    df_product: pl.DataFrame
    df_receipt: pl.DataFrame
    df_store: pl.DataFrame
    df_geocode: pl.DataFrame


def load_100knock_data(dataset_dir: str) -> Dataset:
    """
    - df_customer: 顧客データ
    - df_category: 商品カテゴリデータ
    - df_product: 商品データ
    - df_receipt: レシートデータ
    - df_store: 店舗データ
    - df_geocode: 住所データ
    """
    return Dataset(
        df_customer=pl.read_csv(join(dataset_dir, "../data/customer.csv"), dtypes=dtypes),
        df_category=pl.read_csv(join(dataset_dir, "../data/category.csv"), dtypes=dtypes),
        df_product=pl.read_csv(join(dataset_dir, "../data/product.csv"), dtypes=dtypes),
        df_receipt=pl.read_csv(join(dataset_dir, "../data/receipt.csv"), dtypes=dtypes),
        df_store=pl.read_csv(join(dataset_dir, "../data/store.csv"), dtypes=dtypes),
        df_geocode=pl.read_csv(join(dataset_dir, "../data/geocode.csv"), dtypes=dtypes),
    )
