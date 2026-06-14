from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import os

# Cấu hình Database (Sửa lại mật khẩu của bạn)
DB_CONFIG = {
    "dbname": "ecommerce_db",
    "user": "postgres",
    "password": "@Tmd212205", 
    "host": "host.docker.internal", # Dùng host này để Airflow trong Docker gọi ra DB ngoài Windows
    "port": "5432"
}

# --- CÁC HÀM XỬ LÝ PYTHON ---

def load_csv_to_postgres(file_path, schema_table):
    """Hàm nạp dữ liệu siêu tốc bằng COPY"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Xóa dữ liệu cũ trước khi nạp mới (để tránh trùng lặp khi chạy hằng ngày)
    cur.execute(f"TRUNCATE TABLE {schema_table};")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        copy_sql = f"COPY {schema_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        cur.copy_expert(sql=copy_sql, file=f)
        
    conn.commit()
    cur.close()
    conn.close()

def run_kmeans_clustering():
    """Hàm chạy K-Means và lưu kết quả"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Kéo dữ liệu từ View đã Transform
    query = "SELECT * FROM analytics.vw_rfm_features;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Scale và Phân cụm
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df[['recency', 'frequency', 'monetary']])
    
    kmeans = KMeans(n_clusters=4, init='k-means++', random_state=42)
    df['cluster'] = kmeans.fit_predict(scaled_features)
    
    # Lưu kết quả phân cụm ra thư mục data của Airflow
    output_path = '/opt/airflow/data/customer_segments_result.csv'
    df.to_csv(output_path, index=False)
    print(f"Đã lưu kết quả phân cụm tại {output_path}")

# --- ĐỊNH NGHĨA DAG ---

default_args = {
    'owner': 'nham_trong_du',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='ecommerce_end_to_end_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval='0 2 * * *', # Chạy tự động vào 2h sáng mỗi ngày
    catchup=False,
    tags=['ecommerce', 'etl', 'machine_learning']
) as dag:

    # 1. NHÓM TASK LOAD DATA
    load_users = PythonOperator(
        task_id='load_dim_users',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/processed/dim_users.csv', 'schema_table': 'analytics.dim_users'}
    )

    load_products = PythonOperator(
        task_id='load_dim_products',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/processed/dim_products.csv', 'schema_table': 'analytics.dim_products'}
    )

    load_orders = PythonOperator(
        task_id='load_fact_orders',
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/processed/fact_order_items.csv', 'schema_table': 'analytics.fact_order_items'}
    )

    # 2. TASK TRANSFORM DỮ LIỆU BẰNG SQL (Ép kiểu chuẩn)
    # Task này đảm bảo data nạp vào dạng VARCHAR sẽ được chuyển thành NUMERIC/TIMESTAMP
    transform_data_types = PostgresOperator(
        task_id='transform_and_cast_types',
        postgres_conn_id='ecommerce_pg_conn', # Sẽ cấu hình trên Web UI
        sql="""
            -- (Bạn có thể đưa các lệnh ALTER TABLE ép kiểu vào đây nếu chưa chạy)
            -- Hoặc chạy lệnh Refresh Materialized View nếu dùng.
            SELECT 1; -- Tạm thời giữ chỗ kiểm tra kết nối
        """
    )

    # 3. TASK MACHINE LEARNING
    run_ml_model = PythonOperator(
        task_id='run_rfm_kmeans_clustering',
        python_callable=run_kmeans_clustering
    )

    # --- THIẾT LẬP THỨ TỰ CHẠY (DEPENDENCIES) ---
    # Load 3 file song song -> Xong hết thì Transform -> Xong thì chạy Machine Learning
    [load_users, load_products, load_orders] >> transform_data_types >> run_ml_model