import duckdb
import pandas as pd
import os
from sklearn.ensemble import IsolationForest
import joblib  # Для сохранения самой модели


class AnomalyDetector:
    def __init__(self, contamination=0.01):
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.con = duckdb.connect()

    def load_data(self, file_path):
        """Загрузка данных через SQL"""
        query = f"""
        SELECT cpu_load, ram_usage, 
               hour(timestamp) as hour_of_day, 
               dayofweek(timestamp) as day_of_week
        FROM '{file_path}'
        """
        return self.con.execute(query).df()

    def train(self, df: pd.DataFrame) -> None:
        """Обучение модели"""
        print("🧠 Обучение модели Isolation Forest...")
        self.model.fit(df)

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Предсказание и возврат результата (копируем данные, чтобы не портить оригинал)"""
        # 1. Создаем копию, чтобы не менять исходный df во время предсказания
        data_to_predict = df.copy()

        # 2. Делаем предсказание
        predictions = self.model.predict(data_to_predict)

        # 3. Добавляем результат в копию
        data_to_predict['anomaly_score'] = predictions

        return data_to_predict

    def save_model(self, path='models/anomaly_model.joblib'):
        """Сохранение обученной модели (весов)"""
        os.makedirs('models', exist_ok=True)
        joblib.dump(self.model, path)
        print(f"💾 Модель сохранена в {path}")

    def plot_results(self, df: pd.DataFrame):
        """
        Визуализация результатов: синий — норма, красный — аномалия.
        Сохраняет график в папку reports/.
        """
        import matplotlib.pyplot as plt
        import os

        print("📊 Построение графиков...")
        plt.figure(figsize=(12, 6))

        # Рисуем точки
        plt.scatter(
            df['cpu_load'],
            df['ram_usage'],
            c=(df['anomaly_score'] == -1),
            cmap='coolwarm',
            alpha=0.6
        )

        plt.title("DeepWatch Engine: Anomaly Detection Result")
        plt.xlabel("CPU Load (%)")
        plt.ylabel("RAM Usage (%)")
        plt.grid(True, alpha=0.3)

        # Создаем папку и сохраняем
        os.makedirs('reports', exist_ok=True)
        plt.savefig('reports/anomaly_chart.png', dpi=300)
        print("📈 График сохранен в reports/anomaly_chart.png")

        # Показываем окно (то, что удалили)
        plt.show()

