import os
from src.generator import generate_server_metrics
from src.models import AnomalyDetector
import matplotlib
"""

"""

def main():
    # Настраиваем пути относительно корня
    DATA_PATH = 'data/server_metrics.parquet'
    MODEL_PATH = 'models/anomaly_model.joblib'

    print("🚀 DeepWatch Engine: Starting Pipeline...")

    # 1. Сбор данных
    if not os.path.exists(DATA_PATH):
        print("📥 Data not found. Generating...")
        os.makedirs('data', exist_ok=True)
        df = generate_server_metrics(days=30)
        df.to_parquet(DATA_PATH)

    # 2. Инициализация и работа модели
    # (Мы используем наш ООП-класс из src/models.py)
    detector = AnomalyDetector(contamination=0.01)

    # Загружаем через DuckDB (метод внутри класса)
    raw_data = detector.load_data(DATA_PATH)

    if raw_data.isnull().values.any():
        print("⚠️ Внимание: Обнаружены пропуски в данных! Очистка...")
        raw_data = raw_data.dropna()

    # Обучаем
    detector.train(raw_data)

    # Предсказываем
    results = detector.predict(raw_data)

    # 3. Сохранение результата
    detector.save_model(MODEL_PATH)

    anomalies_count = len(results[results['anomaly_score'] == -1])
    print(f"✅ Pipeline Finished! Found {anomalies_count} anomalies.")
    print(f"📊 Model weights saved to {MODEL_PATH}")

    results = detector.predict(raw_data)

    # ВОТ ОН — ПОБЕДНЫЙ ВЫЗОВ:
    detector.plot_results(results)

    detector.save_model(MODEL_PATH)


if __name__ == "__main__":
    main()
