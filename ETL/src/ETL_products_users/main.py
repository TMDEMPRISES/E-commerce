from extract import load_raw_data
from transform.transform_users import transform_users_data
from transform.transform_products import transform_products_data
from load import save_processed_data
import os
def run_full_pipeline():
    print(os.getcwd())
    # --- Cấu hình đường dẫn ---
    config = {
        'users': {
            'input': 'ETL/data/raw/customers.csv',
            'output': 'ETL/data/processed/dim_users.csv'
        },
        'products': {
            'input': 'ETL/data/raw/products.csv',
            'output': 'ETL/data/processed/dim_products.csv'
        }
    }

    # --- Xử lý bảng USERS ---
    print("--- Đang xử lý bảng Users ---")
    users_raw = load_raw_data(config['users']['input'])
    if users_raw is not None:
        users_clean = transform_users_data(users_raw)
        save_processed_data(users_clean, config['users']['output'])

    # --- Xử lý bảng PRODUCTS ---
    print("\n--- Đang xử lý bảng Products ---")
    products_raw = load_raw_data(config['products']['input'])
    if products_raw is not None:
        products_clean = transform_products_data(products_raw)
        save_processed_data(products_clean, config['products']['output'])

    print("\nToàn bộ Pipeline đã hoàn thành!")

if __name__ == "__main__":
    run_full_pipeline()