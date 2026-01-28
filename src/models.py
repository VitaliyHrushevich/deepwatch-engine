import pandas as pd
import duckdb
import joblib
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional
from sklearn.ensemble import IsolationForest


class AnomalyDetector:
    """
    Advanced Anomaly Detection Engine using Isolation Forest and DuckDB.
    """

    def __init__(self, contamination: float = 0.01):
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.con = duckdb.connect()
        self.is_fitted = False

    def load_data(self, file_path: Path) -> pd.DataFrame:
        """High-performance data loading with feature engineering via SQL."""
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found at {file_path}")

        query = f"""
        SELECT cpu_load, ram_usage, 
               hour(timestamp) as hour_of_day, 
               dayofweek(timestamp) as day_of_week
        FROM '{file_path}'
        """
        return self.con.execute(query).df()

    def train(self, df: pd.DataFrame) -> None:
        """Trains the Isolation Forest model."""
        print("🧠 Training Isolation Forest model...")
        self.model.fit(df)
        self.is_fitted = True

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Performs anomaly detection and returns enriched DataFrame."""
        if not self.is_fitted:
            raise RuntimeError("Model must be trained before prediction.")

        results = df.copy()
        results['anomaly_score'] = self.model.predict(df)
        return results

    def save_model(self, path: Path = Path('models/anomaly_model.joblib')) -> None:
        """Persists the trained model weights."""
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.model, path)
        print(f"💾 Model weight persisted at: {path}")

    def plot_results(self, df: pd.DataFrame, save_dir: Path = Path('reports')):
        """Visualizes anomalies: Blue for Inliers, Red for Outliers."""
        print("📊 Rendering anomaly distribution charts...")
        save_dir.mkdir(parents=True, exist_ok=True)

        plt.figure(figsize=(12, 7))
        plt.scatter(
            df['cpu_load'],
            df['ram_usage'],
            c=(df['anomaly_score'] == -1),
            cmap='coolwarm',
            alpha=0.6,
            edgecolors='k',
            linewidth=0.5
        )

        plt.title("DeepWatch Engine: Multi-dimensional Anomaly Detection")
        plt.xlabel("CPU Load (%)")
        plt.ylabel("RAM Usage (%)")
        plt.colorbar(label="Anomaly (Red) vs Normal (Blue)")
        plt.grid(True, linestyle='--', alpha=0.5)

        file_path = save_dir / 'anomaly_chart.png'
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        print(f"📈 Insight saved at: {file_path}")
        plt.show()
