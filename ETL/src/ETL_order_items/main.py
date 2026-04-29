from extract_order_items import extract_data
from transform_order_items import transform_fact_order_items
from load_order_items import load_data
import os
def run_etl():
    print(os.getcwd())
    path={
        'order_items':'ETL/data/raw/order_items.csv',
        'orders':'ETL/data/raw/orders.csv',
        'customers':'ETL/data/raw/customers.csv',
        'output':'ETL/data/processed/fact_order_items.csv'
    }
    PATH_ITEMS = 'order_items.csv'
    PATH_ORDERS = 'orders.csv'
    PATH_CUSTOMERS = 'customers.csv'
    PATH_FACT_OUTPUT = 'fact_order_items.csv'

    try:
        #Extract
        df_items = extract_data(path['order_items'])
        df_orders = extract_data(path['orders'])
        df_customers = extract_data(path['customers'])

        #Transform
        fact_df = transform_fact_order_items(df_items, df_orders, df_customers)

        #Load
        load_data(fact_df, path['output'])

        print("\n=> Success!!")

    except Exception as e:
        print(f"\n[ETL] Fail: {str(e)}")

if __name__ == "__main__":
    run_etl()