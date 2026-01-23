import duckdb
import pandas as pd
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
import os


def save_results_to_db(df):
    # Saves found anomalies to database and file
    con = duckdb.connect()

    # Leave only anomaly strings (-1)
    anomalies_only = df[df['anomaly_score'] == -1]

    print(f"💾 Сохраняю {len(anomalies_only)} аномалий в отчет...")

    # DuckDB magic: create table directly from Pandas DataFrame 'anomalies_only'
    con.execute("CREATE TABLE IF NOT EXISTS results AS SELECT * FROM anomalies_only")

    # Export to Parquet for final report
    report_path = '../data/final_anomalies_report.parquet'
    con.execute(f"COPY (SELECT * FROM anomalies_only) TO '{report_path}' (FORMAT PARQUET)")
    print(f"✅ Отчет готов: {report_path}")


def train_and_detect():
    # 1. Loading data via SQL
    con = duckdb.connect()
    print("📥 Загрузка признаков из DuckDB...")

    query = """
    SELECT 
        cpu_load, 
        ram_usage, 
        hour(timestamp) as hour_of_day,
        dayofweek(timestamp) as day_of_week
    FROM '../data/server_metrics.parquet'
    """
    df = con.execute(query).df()

    # 2. Model training
    print("🧠 Обучаю Isolation Forest (леса изоляции)...")
    model = IsolationForest(contamination=0.01, random_state=42)

    # Learning all data (looking for emissions)
    model.fit(df)

    # 3. Predictions
    df['anomaly_score'] = model.predict(df)

    # 4. Saving results
    save_results_to_db(df)

    # 5. Visualization
    print("📊 Строю графики...")
    plt.figure(figsize=(12, 6))
    # Color: blue - normal, red - anomaly
    plt.scatter(df['cpu_load'], df['ram_usage'],
                c=(df['anomaly_score'] == -1), cmap='coolwarm', alpha=0.6)

    plt.title("DeepWatch Engine: CPU vs RAM Anomaly Detection")
    plt.xlabel("Нагрузка CPU (%)")
    plt.ylabel("Использование RAM (%)")
    plt.grid(True, alpha=0.3)
    plt.show()


if __name__ == "__main__":
    train_and_detect()
