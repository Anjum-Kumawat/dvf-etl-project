from dagster import Definitions

from .assets import (
    ban_bronze, dpe_bronze, dvf_bronze, dvf_pipeline_job,
    dvf_publication_schedule, filosofi_bronze, geo_bronze,
    gold_price_aggregates, silver_dvf,
)
from .sensors import pipeline_failure_sensor

defs = Definitions(
    assets=[dvf_bronze, ban_bronze, dpe_bronze, filosofi_bronze, geo_bronze, silver_dvf, gold_price_aggregates],
    jobs=[dvf_pipeline_job],
    schedules=[dvf_publication_schedule],
    sensors=[pipeline_failure_sensor],
)