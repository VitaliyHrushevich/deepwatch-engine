import duckdb
from pathlib import Path

# Path constants
DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "server_metrics.parquet"


def extract_critical_metrics(threshold: float = 90.0):
    """
    Performs high performance data filtering using DuckDB.
    """
    if not DATA_PATH.exists():
        print(f"❌ Файл не найден: {DATA_PATH}")
        return

    # Connect to DuckDB (in-memory)
    con = duckdb.connect()

    query = f"""
    SELECT 
        timestamp, 
        cpu_load,
        ram_usage
    FROM '{DATA_PATH}'
    WHERE cpu_load > {threshold}
    ORDER BY timestamp
    """

    print(f"🔍 Сканирование Parquet... Фильтр: CPU > {threshold}%")

    try:
        df_critical = con.execute(query).df()

        if not df_critical.empty:
            print(f"✅ Найдено {len(df_critical)} критических записей.")
            return df_critical
        else:
            print("📭 Критических скачков не обнаружено.")
            return None
    except Exception as e:
        print(f"🛑 Ошибка при выполнении SQL: {e}")


if __name__ == "__main__":
    extract_critical_metrics()
