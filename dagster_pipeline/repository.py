from dagster import repository
from .pipeline import sales_etl_pipeline

@repository
def sales_repository():
    return [sales_etl_pipeline]
