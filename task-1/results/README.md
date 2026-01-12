1. Выбор технологического решения
   ✅ Выбранное решение: Apache Airflow

Почему Airflow оптимален для данной задачи:

| Требование                        | Поддержка в Airflow               |
|-----------------------------------|-----------------------------------|
| Пакетная обработка ~1 млн записей | Поддерживает                      |
| Гибкий пайплайн (DAG)             | DAG на Python                     |
| Ветвление и условия               | BranchPythonOperator              |
| Retry, fallback-логика            | Поддерживает                      |
| Email-уведомления                 | Поддерживает                      |
| Мониторинг                        | Поддерживает Web UI               |
| Интеграция с Spark                | SparkSubmitOperator               |
| Интеграция с Kafka                | Kafka consumer/producer operators |
| Поддержка BigQuery                | airflow-providers-google          |
| Поддержка Redshift                | airflow-providers-amazon          |
| Поддержка Postgresql              | PostgresOperator                  |
| Масштабирование                   | Celery / Kubernetes               |
| Развертывание в облаке            | Kubernetes                        |

