# 📈 DeepWatch Engine: High-Precision Anomaly Detection

![Anomaly Detection Chart](reports/anomaly_chart.png)

**DeepWatch Engine** is a high-performance infrastructure monitoring system designed to detect system outliers (anomalies) in real-time telemetry. By fusing **DuckDB** for efficient data handling and **Isolation Forest** for unsupervised learning, it provides a robust defense against "Black Swan" events in server clusters.

## 🌟 Key Features
- **High-Performance Ingestion:** Direct SQL querying over **Parquet** files using DuckDB (zero-overhead loading).
- **Advanced Feature Engineering:** Automated time-context extraction (Hour of Day, Day of Week) via SQL.
- **Unsupervised Detection:** Leverages **Isolation Forest** to identify complex, multi-dimensional anomalies without labeled data.
- **Decision Visualization:** Integrated reporting engine that renders anomaly distribution charts for instant ops insight.

## 🛠 Tech Stack
- **ML Engine:** Scikit-learn (Isolation Forest)
- **Data Querying:** DuckDB (High-performance SQL)
- **Core:** Python 3.11, Pandas, NumPy, Pathlib
- **Visuals:** Matplotlib, Seaborn

## 📈 Data Pipeline (Architecture)

1. **Data Ingestion:** High-speed scanning of Parquet telemetry files.
2. **Feature Engineering:** SQL-based extraction of temporal features (Hour/Day).
3. **ML Inference:** Isolation Forest scoring to identify multi-dimensional outliers.
4. **Reporting:** Generation of decision-boundary charts and model persistence.

## 🛡 Business Impact: Why F1-Score?
In anomaly detection, **Accuracy is a lie**. Predicting "Normal" 99% of the time gives 99% accuracy but fails to find the failures. DeepWatch Engine focuses on **F1-Score** (Precision vs. Recall balance) to ensure that critical spikes are never missed while minimizing false alarms.

## 🚀 Quick Start
1. Clone repo: `git clone https://github.com`
2. Install dependencies: `pip install -r requirements.txt`
3. Run the E2E Pipeline: `python main.py`

## 🗺 Future Roadmap
- [ ] **Real-time Streaming:** Integration with Apache Kafka for sub-second anomaly detection on live streams.
- [ ] **Adaptive Thresholding:** Implementing Dynamic Contamination levels based on historical drift.
- [ ] **Explainable AI (XAI):** Adding SHAP/LIME values to explain *why* a specific data point was flagged as an anomaly.
- [ ] **Prometheus Exporter:** Automated metric pushing for Grafana dashboards.

## 🤝 Contributing
Feel free to fork this project, report issues, or suggest new anomaly detection algorithms (e.g., Local Outlier Factor or Autoencoders).
