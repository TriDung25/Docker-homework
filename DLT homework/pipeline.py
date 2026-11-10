import dlt
import requests

URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="ny_taxi", write_disposition="replace")
def ny_taxi():
    page = 1
    while True:
        print(f"Đang lấy trang {page}...")
        resp = requests.get(URL, params={"page": page}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print("Data ran out")
            break
        yield data
        page += 1


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data",
)

load_info = pipeline.run(ny_taxi)
print(load_info)

with pipeline.sql_client() as client:
    print("Số dòng:", client.execute_sql("SELECT COUNT(*) FROM ny_taxi"))
    print("Lịch sử load:", client.execute_sql(
        "SELECT load_id, status, inserted_at FROM _dlt_loads"
    ))