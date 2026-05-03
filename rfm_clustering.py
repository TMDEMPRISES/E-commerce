import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# 1. Cấu hình kết nối Database (Giống hệt file local_load.py của bạn)
DB_CONFIG = {
    "dbname": "ecommerce_db",
    "user": "postgres",
    "password": "@Tmd212205", 
    "host": "localhost",
    "port": "5432"
}

def run_rfm_clustering():
    print("1. Đang kết nối Database và lấy dữ liệu RFM...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Dùng Pandas đọc thẳng từ View SQL bạn vừa tạo
    query = "SELECT * FROM analytics.vw_rfm_features;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"-> Đã lấy thành công {len(df)} khách hàng.\n")

    # 2. Tiền xử lý dữ liệu (Cực kỳ quan trọng)
    # Vì R (ngày), F (lần), M (tiền) có đơn vị khác nhau hoàn toàn. 
    # Nếu không đưa về cùng một hệ quy chiếu (Scale), cột Tiền sẽ lấn át tất cả.
    print("2. Đang chuẩn hóa (Scale) dữ liệu...")
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df[['recency', 'frequency', 'monetary']])

    # 3. Chạy thuật toán K-Means
    # Giả sử chúng ta muốn chia tập khách hàng thành 4 nhóm (K=4)
    print("3. Đang huấn luyện mô hình K-Means với 4 cụm...")
    kmeans = KMeans(n_clusters=4, init='k-means++', random_state=42)
    
    # Gắn nhãn (Cluster) cho từng khách hàng
    df['cluster'] = kmeans.fit_predict(scaled_features)

    # 4. Phân tích kết quả
    print("\n4. Kết quả phân cụm (Trung bình của từng nhóm):")
    # Groupby theo cluster để xem đặc điểm của mỗi nhóm
    summary = df.groupby('cluster').agg({
        'recency': 'mean',
        'frequency': 'mean',
        'monetary': ['mean', 'count'] # Xem cả số lượng khách trong nhóm
    }).round(2)
    
    print(summary)

    # 5. Lưu kết quả ra file CSV để báo cáo
    output_file = "customer_segments.csv"
    df.to_csv(output_file, index=False)
    print(f"\n Đã lưu chi tiết từng khách hàng ra file: {output_file}")

if __name__ == "__main__":
    run_rfm_clustering()