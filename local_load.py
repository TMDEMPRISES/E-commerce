import psycopg2
import time

# Cấu hình Database
DB_CONFIG = {
    "dbname": "ecommerce_db",
    "user": "postgres",
    "password": "@Tmd212205", # Điền pass của bạn
    "host": "localhost",
    "port": "5432"
}

def load_csv_fast(file_path, schema_table):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print(f"Đang nạp {file_path} vào {schema_table}...")
    start_time = time.time()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Lệnh COPY chuyên dụng
        copy_sql = f"COPY {schema_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        try:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            print(f"Xong! Mất {round(time.time() - start_time, 2)} giây.\n")
        except Exception as e:
            conn.rollback()
            print(f"Lỗi: {e}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    # NẠP TỪ THƯ MỤC PROCESSED VÀO SCHEMA ANALYTICS
    load_csv_fast(r"C:\Users\Hi\Desktop\E-commerce\ETL\data\processed\fact_order_items.csv", "analytics.fact_order_items")
    load_csv_fast(r"C:\Users\Hi\Desktop\E-commerce\ETL\data\processed\dim_products.csv", "analytics.dim_products")
    load_csv_fast(r"C:\Users\Hi\Desktop\E-commerce\ETL\data\processed\dim_users.csv", "analytics.dim_users")