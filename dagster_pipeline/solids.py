import pandas as pd
from dagster import solid

@solid
def load_sales_data(context):
    df = pd.read_csv("data/sales.csv")
    context.log.info(f"Loaded {len(df)} sales records")
    return df

@solid
def load_customers_data(context):
    df = pd.read_csv("data/customers.csv")
    context.log.info(f"Loaded {len(df)} customers")
    return df

@solid
def load_products_data(context):
    df = pd.read_csv("data/products.csv")
    context.log.info(f"Loaded {len(df)} products")
    return df

@solid
def clean_sales_data(context, sales_df):
    df = sales_df.drop_duplicates()
    df['date'] = pd.to_datetime(df['date'])
    context.log.info(f"Cleaned sales data, {len(df)} rows after cleaning")
    return df

@solid
def transform_data(context, sales_df, customers_df, products_df):
    df = sales_df.merge(customers_df, on='customer_id').merge(products_df, on='product_id')
    df['total'] = df['quantity'] * df['price']
    context.log.info("Transformed data with total sales column")
    return df

@solid
def save_output(context, df):
    df.to_csv("data/sales_report.csv", index=False)
    context.log.info("Saved transformed sales report to data/sales_report.csv")
    return "data/sales_report.csv"
