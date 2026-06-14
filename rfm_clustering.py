import os
import logging

import psycopg2
import psycopg2.extras
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Connection string lấy từ biến môi trường, không hardcode trong source.
NEON_DB_URL = "postgresql://neondb_owner:npg_vhRKtaxTw09r@ep-sweet-king-aoq24ws3-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

N_CLUSTERS = 4

# Thứ tự xếp hạng segment theo điểm "giá trị khách hàng" giảm dần.
# Phải khớp với SEGMENT_ORDER trong dashboard.
SEGMENT_RANK_ORDER = ["best", "loyal", "at_risk", "churned"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.rfm_customers (
    customer_id   VARCHAR(50) PRIMARY KEY,
    recency       INT,
    frequency     INT,
    monetary      FLOAT,
    cluster       INT,
    segment_label VARCHAR(20),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

UPSERT_SQL = """
INSERT INTO analytics.rfm_customers
    (customer_id, recency, frequency, monetary, cluster, segment_label)
VALUES %s
ON CONFLICT (customer_id) DO UPDATE SET
    recency       = EXCLUDED.recency,
    frequency     = EXCLUDED.frequency,
    monetary      = EXCLUDED.monetary,
    cluster       = EXCLUDED.cluster,
    segment_label = EXCLUDED.segment_label,
    updated_at    = CURRENT_TIMESTAMP;
"""


def load_features(conn) -> pd.DataFrame:
    """Đọc và validate dữ liệu từ view analytics.vw_rfm_features."""
    logger.info("Đang lấy dữ liệu từ view analytics.vw_rfm_features ...")
    df = pd.read_sql_query("SELECT * FROM analytics.vw_rfm_features;", conn)
    logger.info("-> Đã lấy %d dòng.", len(df))

    if df.empty:
        raise ValueError("View analytics.vw_rfm_features không trả về dữ liệu.")

    # Chuẩn hoá tên cột khoá: chấp nhận cả 'customer_id' hoặc 'user_id'
    if "customer_id" not in df.columns:
        if "user_id" in df.columns:
            df = df.rename(columns={"user_id": "customer_id"})
        else:
            raise KeyError(
                "Không tìm thấy cột 'customer_id' hoặc 'user_id' trong view nguồn."
            )

    required_cols = {"customer_id", "recency", "frequency", "monetary"}
    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"View thiếu các cột bắt buộc: {missing}")

    # NaN trong recency/frequency/monetary sẽ làm hỏng StandardScaler/KMeans
    before = len(df)
    df = df.dropna(subset=["recency", "frequency", "monetary"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.warning("Đã loại %d dòng do thiếu recency/frequency/monetary.", dropped)

    if df.empty:
        raise ValueError("Không còn dữ liệu hợp lệ sau khi loại bỏ NaN.")

    return df


def run_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hoá dữ liệu RFM và chạy K-Means."""
    logger.info("Đang chuẩn hoá dữ liệu và chạy K-Means (k=%d) ...", N_CLUSTERS)

    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[["recency", "frequency", "monetary"]])

    kmeans = KMeans(n_clusters=N_CLUSTERS, init="k-means++", random_state=42, n_init=10)

    df = df.copy()
    df["cluster"] = kmeans.fit_predict(scaled)
    return df


def assign_segment_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gán segment_label dựa trên đặc tính trung bình của từng cluster, KHÔNG
    dựa vào cluster ID (vì ID này thay đổi ngẫu nhiên giữa các lần chạy).

    score = frequency_norm + monetary_norm + (1 - recency_norm)
    -> điểm cao nhất = khách hàng giá trị nhất ("best").
    """
    summary = df.groupby("cluster").agg(
        recency=("recency", "mean"),
        frequency=("frequency", "mean"),
        monetary=("monetary", "mean"),
        count=("customer_id", "count"),
    )

    def normalize(s: pd.Series) -> pd.Series:
        rng = s.max() - s.min()
        if rng == 0:
            return pd.Series(0.5, index=s.index)
        return (s - s.min()) / rng

    score = (
        normalize(summary["frequency"])
        + normalize(summary["monetary"])
        + (1 - normalize(summary["recency"]))
    )

    ranking = score.sort_values(ascending=False).index.tolist()

    # Map an toàn cả khi số cluster thực tế khác len(SEGMENT_RANK_ORDER)
    cluster_to_label = {
        cluster_id: SEGMENT_RANK_ORDER[min(rank, len(SEGMENT_RANK_ORDER) - 1)]
        for rank, cluster_id in enumerate(ranking)
    }

    logger.info("Kết quả phân cụm (trung bình theo cluster):\n%s", summary.round(2))
    logger.info("Gán nhãn segment: %s", cluster_to_label)

    df = df.copy()
    df["segment_label"] = df["cluster"].map(cluster_to_label)
    return df


def save_to_db(conn, df: pd.DataFrame) -> None:
    """Upsert kết quả vào analytics.rfm_customers (không drop table)."""
    logger.info("Đang upsert %d bản ghi vào analytics.rfm_customers ...", len(df))

    cursor = conn.cursor()
    try:
        cursor.execute(CREATE_TABLE_SQL)

        data = list(
            df[["customer_id", "recency", "frequency", "monetary", "cluster", "segment_label"]]
            .astype(
                {
                    "customer_id": str,
                    "recency": int,
                    "frequency": int,
                    "monetary": float,
                    "cluster": int,
                    "segment_label": str,
                }
            )
            .itertuples(index=False, name=None)
        )

        psycopg2.extras.execute_values(cursor, UPSERT_SQL, data, page_size=1000)
        conn.commit()
        logger.info("Đã lưu thành công %d bản ghi.", len(df))
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def run_rfm_clustering() -> None:
    if not NEON_DB_URL:
        raise RuntimeError(
            "Chưa cấu hình biến môi trường RFM_ADMIN_DB_URL. "
            "Hãy chạy: export RFM_ADMIN_DB_URL='postgresql://...'"
        )

    logger.info("Đang kết nối tới Neon Database ...")
    conn = psycopg2.connect(NEON_DB_URL)
    try:
        df = load_features(conn)
        df = run_kmeans(df)
        df = assign_segment_labels(df)
        save_to_db(conn, df)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run_rfm_clustering()
    except Exception as e:
        logger.error("Lỗi: %s", e)
        raise
