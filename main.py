import sys
import os
from pathlib import Path

# 1. Path configuration
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

from src.generator import generate_server_metrics
from src.models import AnomalyDetector

def main():
    # Configure paths through Pathlib for reliability
    DATA_DIR = BASE_DIR / "data"
    MODEL_DIR = BASE_DIR / "models"
    DATA_PATH = DATA_DIR / "server_metrics.parquet"
    MODEL_PATH = MODEL_DIR / "anomaly_model.joblib"

    print("🚀 DeepWatch Engine: Initializing Pipeline...")

    # 1. Data Ingestion & Generation
    if not DATA_PATH.exists():
        print("📥 Telemetry data not found. Running generator...")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df = generate_server_metrics(days=30)
        df.to_parquet(DATA_PATH)
    else:
        print(f"✅ Telemetry data found at {DATA_PATH}")

    # 2. Model Orchestration
    detector = AnomalyDetector(contamination=0.01)

    raw_data = detector.load_data(DATA_PATH)

    # Data Quality Check
    if raw_data.isnull().values.any():
        print("⚠️ Data Quality Alert: Missing values detected. Cleaning...")
        raw_data = raw_data.dropna()

    # ML Workflow: Train -> Predict -> Save
    detector.train(raw_data)
    results = detector.predict(raw_data)
    detector.save_model(MODEL_PATH)

    # 3. Output & Insights
    anomalies_count = len(results[results['anomaly_score'] == -1])
    print(f"\n" + "="*40)
    print(f"✅ PIPELINE FINISHED!")
    print(f"🔹 Detected Anomalies: {anomalies_count}")
    print(f"🔹 Model Artifact: {MODEL_PATH}")
    print("="*40)

    detector.plot_results(results)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"🛑 Critical Pipeline Failure: {e}")
