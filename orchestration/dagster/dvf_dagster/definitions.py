from dagster import Definitions

from .assets import (
    ban_bronze,
    dpe_bronze,
    dvf_bronze,
    filosofi_bronze,
    geo_bronze,
    gold_price_aggregates,
    silver_dvf,
)

defs = Definitions(
    assets=[
        dvf_bronze,
        ban_bronze,
        dpe_bronze,
        filosofi_bronze,
        geo_bronze,
        silver_dvf,
        gold_price_aggregates,
    ]
)