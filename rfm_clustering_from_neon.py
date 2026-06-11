import psycopg2
import psycopg2.extras
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

NEON_DB_URL = "ADMINURL"

def run_rfm_clustering():
    try:
        print("Đang kết nối tới Neon Database...")
        conn = psycopg2.connect(NEON_DB_URL)
        
        # 1. ĐỌC DỮ LIỆU TỪ NEON
        print("1. Đang lấy dữ liệu từ View analytics.vw_rfm_features...")
        query = "SELECT * FROM analytics.vw_rfm_features;"
        df = pd.read_sql_query(query, conn)
        print(f"-> Đã lấy thành công {len(df)} khách hàng.\n")

        # 2. XỬ LÝ & PHÂN CỤM (K-MEANS)
        print("2. Đang chuẩn hóa dữ liệu và chạy thuật toán K-Means...")
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(df[['recency', 'frequency', 'monetary']])

        kmeans = KMeans(n_clusters=4, init='k-means++', random_state=42)
        df['cluster'] = kmeans.fit_predict(scaled_features)

        print("\n3. Kết quả phân cụm (Trung bình):")
        summary = df.groupby('cluster').agg({
            'recency': 'mean',
            'frequency': 'mean',
            'monetary': ['mean', 'count']
        }).round(2)
        print(summary)

        # 4. GHI DỮ LIỆU NGƯỢC LẠI LÊN NEON
        print("\n4. Đang lưu siêu tốc (Bulk Insert) bảng analytics.rfm_customers lên Neon...")
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS analytics.rfm_customers;")
        
        create_table_query = """
        CREATE TABLE analytics.rfm_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            recency INT,
            frequency INT,
            monetary FLOAT,
            cluster INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
                
        # Chuyển đổi DataFrame
        data_tuples = [
            (str(row['user_id']), int(row['recency']), int(row['frequency']), float(row['monetary']), int(row['cluster']))
            for index, row in df.iterrows()
        ]
        
        # Câu lệnh Insert
        insert_query = """
            INSERT INTO analytics.rfm_customers (customer_id, recency, frequency, monetary, cluster)
            VALUES %s
        """
        
        # Gọi execute_values
        psycopg2.extras.execute_values(
            cursor, 
            insert_query, 
            data_tuples, 
            page_size=1000
        )
                
        conn.commit()
        print(f"Đã lưu thành công {len(df)} bản ghi lên Neon trong chớp mắt!")
        
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    run_rfm_clustering()