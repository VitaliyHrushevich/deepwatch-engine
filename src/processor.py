import duckdb


def find_anomalies():
    con = duckdb.connect()

    # Этот запрос ищет все моменты, где нагрузка была экстремальной
    query = """
    SELECT 
        timestamp, 
        cpu_load 
    FROM '../data/server_metrics.parquet'
    WHERE cpu_load > 90
    ORDER BY timestamp
    """

    print("🔍 Ищу критические скачки нагрузки (CPU > 90%)...")
    df_anomalies = con.execute(query).df()
    print(df_anomalies)


if __name__ == "__main__":
    find_anomalies()
