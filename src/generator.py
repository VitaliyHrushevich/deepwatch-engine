import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os


def generate_server_metrics(days=30):
    # 1. Preparation of time scale
    start_time = datetime(2026, 1, 1)
    n_points = days * 24 * 60  # Minute interval
    timestamps = [start_time + timedelta(minutes=i) for i in range(n_points)]

    time_idx = np.arange(n_points)

    # 2. Generate normal behavior (sinusoid + trend + noise)
    # Daytime cycle: peak during the day, decline at night
    seasonal = 20 * np.sin(2 * np.pi * time_idx / 1440) + 50
    # Small growing trend (The service is becoming more popular)
    trend = 0.0001 * time_idx
    # Random noise
    noise = np.random.normal(0, 3, n_points)

    cpu_load = seasonal + trend + noise

    # 3. Introducing "Enemy" (Anomalies)
    # Select random indices for 5 major failures
    anomaly_indices = np.random.choice(range(n_points), size=5, replace=False)
    for idx in anomaly_indices:
        # Surge load to 95-99% for 30 minutes
        cpu_load[idx:idx + 30] = np.random.uniform(95, 99)

    # 4. RAM usage (usually correlates with CPU, but with delay)
    ram_usage = cpu_load * 0.8 + np.random.normal(10, 2, n_points)

    # Limit values to physical limits
    cpu_load = np.clip(cpu_load, 0, 100)
    ram_usage = np.clip(ram_usage, 0, 100)

    # Collect DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'server_id': 'srv_alpha_01',
        'cpu_load': cpu_load,
        'ram_usage': ram_usage
    })

    return df


if __name__ == "__main__":
    # Create a folder for the data if it is missing
    os.makedirs('../data', exist_ok=True)

    print("🚀 Generating data...")
    data = generate_server_metrics(days=30)

    # Save in efficient format Parquet
    file_path = '../data/server_metrics.parquet'
    data.to_parquet(file_path, engine='pyarrow')

    print(f"✅ Success! File saved at {file_path}")
    print(data.head())
