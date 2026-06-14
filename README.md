Hướng dẫn chạy

1.cấu trúc thư mục

E-commerce/
├── dags/
│   └── ecommerce_master_pipeline.py  (File DAG chứa code Airflow)
├── data/
│   └── processed/                    (Chứa 3 file CSV)
│       ├── dim_users.csv
│       ├── dim_products.csv
│       └── fact_order_items.csv
├── docker-compose.yaml               (File tải từ trang chủ Airflow hoặc trong file cùng tên)
└── requirements.txt                  (File khai báo thư viện)
Lưu ý: các file này nằm bên ngoài thư mục etl,tương tự với các file khác trong branch
_Thay thông tin db như db password

2.Khởi chạy docker trên máy

3.Chạy airlow

Bước 1: Khởi tạo Airflow
Chạy lệnh này để Airflow thiết lập cơ sở dữ liệu nội bộ của nó và cài đặt các thư viện trong requirements.txt:
Bash
docker-compose up airflow-init

Bước 2: Chạy hệ thống ngầm (Deploy)
Khởi động cụm server Airflow:
Bash
docker-compose up -d
Hệ thống lúc này đã được "Deploy" thành công trên môi trường Docker của máy bạn.

Bước 3: Cấu hình Connection trên Web UI
Truy cập http://localhost:8080 (Tài khoản/Mật khẩu mặc định: airflow / airflow).
Vào Menu Admin ➔ Connections ➔ Chọn Add a new record (nút dấu cộng).
Điền thông tin kết nối Database để cái PostgresOperator trong code hoạt động:

Conn Id: ecommerce_pg_conn (Phải gõ chính xác như trong file DAG).

Conn Type: Postgres

Host: host.docker.internal

Schema: ecommerce_db

Login: postgres

Password: Mật khẩu của bạn

Port: 5432

Bấm Save.

Bước 4: Kích hoạt Pipeline
Trở lại trang chủ (DAGs), bạn sẽ thấy ecommerce_end_to_end_pipeline.

Gạt nút công tắc (Toggle) bên trái sang ON (màu xanh).

Bấm nút Play (Trigger DAG) ở bên phải.

Bấm vào DAG, chọn tab Graph để xem luồng dữ liệu chạy trực tiếp. Các khối vuông sẽ chuyển từ Xám (Đang chờ) -> Xanh lá nhạt (Đang chạy) -> Xanh lá đậm (Thành công).
