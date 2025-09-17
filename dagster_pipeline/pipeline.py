from dagster import pipeline
from .solids import load_sales_data, load_customers_data, load_products_data, clean_sales_data, transform_data, save_output

@pipeline
def sales_etl_pipeline():
    sales = load_sales_data()
    customers = load_customers_data()
    products = load_products_data()
    clean_sales = clean_sales_data(sales)
    transformed = transform_data(clean_sales, customers, products)
    save_output(transformed)
