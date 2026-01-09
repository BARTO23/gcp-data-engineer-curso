from google.cloud import bigquery
# Installation Requirements:
# 1. google-cloud-bigquery: Main library for BigQuery operations
# 2. pandas: Required for .to_dataframe() conversion
# 3. db-dtypes: Database type support for pandas conversion
# 4. pyarrow: Optional but recommended for better performance
#
# Note on db-dtypes installation:
# If you encounter "ModuleNotFoundError: No module named 'db_dtypes'",
# use: python3 -m pip install --upgrade --force-reinstall db-dtypes
# This may be necessary when using Python 3.13 from Microsoft Store
# as dependencies might not install automatically.
#
# Alternative if db-dtypes fails to install:
# Replace .to_dataframe() with .result() to avoid pandas requirement

def query_public_dataset():
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # Define the SQL query to retrieve data from a public dataset.
    query = """
        SELECT order_items.id, order_id, product_id, products.name
        FROM `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
        JOIN `bigquery-public-data.thelook_ecommerce.products` AS products
        ON order_items.product_id = products.id
    """

    results = client.query(query).to_dataframe()[:5]
    
    print(results)

if __name__ == "__main__":
    query_public_dataset()