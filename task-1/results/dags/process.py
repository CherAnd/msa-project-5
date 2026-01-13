from datetime import datetime, timedelta
import pandas as pd
import os
import time
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.models import Variable
import logging


default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': [os.getenv('EMAIL_ADMIN', 'admin@example.com')],  # Email адреса из переменных окружения
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'process',
    default_args=default_args,
    description='Пакетная обработка',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['batch_data_processing'],
)


DATA_FILE_PATH = '/opt/airflow/data/data.csv'
OUTPUT_DIR = '/opt/airflow/data/processed'

def send_email(to_emails, subject, html_content, smtp_host=None, smtp_port=None,
               smtp_user=None, smtp_password=None, max_retries=3):
    """
    Функция отправки email с прямым подключением к SMTP и retry механизмом.
    """
    smtp_host = smtp_host or os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = smtp_port or int(os.getenv('SMTP_PORT', '587'))
    smtp_user = smtp_user or os.getenv('SMTP_USER', '')
    smtp_password = smtp_password or os.getenv('SMTP_PASSWORD', '')

    for attempt in range(max_retries):
        try:
            logging.info(f"Попытка отправки email #{attempt + 1}/{max_retries}")

            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = smtp_user
            msg['To'] = ', '.join(to_emails) if isinstance(to_emails, list) else to_emails

            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)

            try:
                server = smtplib.SMTP_SSL(smtp_host, 465, timeout=60)
                logging.info("Используем SSL соединение (порт 465)")
            except:
                logging.info("Используем TLS соединение (порт 587)")

            # Логинимся
            server.login(smtp_user, smtp_password)

            # Отправляем email
            text = msg.as_string()
            server.sendmail(smtp_user, to_emails, text)
            server.quit()

            logging.info(f"Email успешно отправлен на {to_emails} (попытка #{attempt + 1})")
            return True

        except smtplib.SMTPConnectError as e:
            logging.warning(f"Ошибка подключения к SMTP (попытка #{attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Ждем 5 секунд перед следующей попыткой
            else:
                raise
        except smtplib.SMTPAuthenticationError as e:
            logging.error(f"Ошибка аутентификации SMTP: {e}")
            raise
        except smtplib.SMTPException as e:
            logging.warning(f"SMTP ошибка (попытка #{attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
        except Exception as e:
            logging.error(f"Неожиданная ошибка при отправке email (попытка #{attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise

    return False

def read_and_validate_data(**context):
    logging.info("Начинаем чтение данных из файла: %s", DATA_FILE_PATH)

    try:
        # Проверяем существование файла
        if not os.path.exists(DATA_FILE_PATH):
            raise FileNotFoundError(f"Файл данных не найден: {DATA_FILE_PATH}")

        # Читаем данные
        df = pd.read_csv(DATA_FILE_PATH)
        logging.info("Данные успешно загружены. Количество записей: %d", len(df))

        # Базовая валидация
        if df.empty:
            raise ValueError("Файл данных пуст")

        # Проверяем наличие обязательных колонок
        required_columns = ['id', 'name', 'age', 'city', 'salary', 'department']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")

        # Сохраняем данные в XCom для использования в следующих задачах
        context['task_instance'].xcom_push(key='data_info', value={
            'row_count': len(df),
            'columns': list(df.columns),
            'file_path': DATA_FILE_PATH
        })

        logging.info("Валидация данных завершена успешно")
        return True

    except Exception as e:
        logging.error("Ошибка при чтении данных: %s", str(e))
        raise

def analyze_data(**context):
    try:
        data_info = context['task_instance'].xcom_pull(key='data_info', task_ids='read_data')
        df = pd.read_csv(data_info['file_path'])
        analysis_results = {
            'total_records': int(len(df)),
            'avg_salary': float(df['salary'].mean()),
            'max_salary': float(df['salary'].max()),
            'min_salary': float(df['salary'].min()),
            'departments': {str(k): int(v) for k, v in df['department'].value_counts().to_dict().items()},
            'cities': {str(k): int(v) for k, v in df['city'].value_counts().to_dict().items()},
            'high_earn_employees': int(len(df[df['salary'] > 60000])),
            'needs_processing': len(df[df['salary'] > 60000]) > 3  # Условие для ветвления
        }
        context['task_instance'].xcom_push(key='analysis_results', value=analysis_results)

        logging.info("Анализ завершен. Результаты: %s", analysis_results)
        return True

    except Exception as e:
        logging.error("Ошибка при анализе данных: %s", str(e))
        raise

def process_high_earn_employees(**context):

    try:
        data_info = context['task_instance'].xcom_pull(key='data_info', task_ids='read_data')
        df = pd.read_csv(data_info['file_path'])
        high_earn_employees = df[df['salary'] > 60000].copy()
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = os.path.join(OUTPUT_DIR, 'high_earn_employees.csv')
        high_earn_employees.to_csv(output_file, index=False)
        report = {
            'processed_records': int(len(high_earn_employees)),
            'output_file': output_file,
            'avg_salary_high_earn_employees': float(high_earn_employees['salary'].mean()),
            'departments_high_earn_employees': {str(k): int(v) for k, v in high_earn_employees['department'].value_counts().to_dict().items()}
        }

        context['task_instance'].xcom_push(key='processing_results', value=report)
        return report

    except Exception as e:
        logging.error("Ошибка при обработке данных: %s", str(e))
        raise

def process_regular_employees(**context):
    logging.info("Обрабатываем данные о обычных сотрудниках")

    try:
        data_info = context['task_instance'].xcom_pull(key='data_info', task_ids='read_data')
        df = pd.read_csv(data_info['file_path'])
        regular_employees = df[df['salary'] <= 60000].copy()
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = os.path.join(OUTPUT_DIR, 'regular_employees.csv')
        regular_employees.to_csv(output_file, index=False)
        report = {
            'processed_records': int(len(regular_employees)),
            'output_file': output_file,
            'avg_salary_regular': float(regular_employees['salary'].mean()),
            'departments_regular': {str(k): int(v) for k, v in regular_employees['department'].value_counts().to_dict().items()}
        }

        context['task_instance'].xcom_push(key='processing_results', value=report)

        logging.info("Обработка обычных сотрудников завершена: %s", report)
        return report

    except Exception as e:
        logging.error("Ошибка при обработке данных: %s", str(e))
        raise

def generate_final_report(**context):
    logging.info("Генерируем финальный отчет")

    try:
        # Получаем результаты анализа
        analysis_results = context['task_instance'].xcom_pull(key='analysis_results', task_ids='analyze_data')

        # Получаем результаты обработки (если есть)
        processing_results = context['task_instance'].xcom_pull(key='processing_results', task_ids=['process_high_earn_employees', 'process_regular_employees'])

        # Создаем финальный отчет
        final_report = {
            'timestamp': datetime.now().isoformat(),
            'analysis_summary': analysis_results,
            'processing_summary': processing_results,
            'status': 'completed_successfully'
        }

        # Сохраняем отчет
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        report_file = os.path.join(OUTPUT_DIR, f'final_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

        import json
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)

        context['task_instance'].xcom_push(key='final_report', value=final_report)

        logging.info("Финальный отчет создан: %s", report_file)
        return final_report

    except Exception as e:
        logging.error("Ошибка при создании финального отчета: %s", str(e))
        raise
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASS = os.getenv('SMTP_PASSWORD', '')
MAIL_FROM = os.getenv('SMTP_MAIL_FROM', SMTP_USER)

def send_success_notification(**context):
    """
    Отправляет уведомление об успешном завершении.
    """
    logging.info("Отправляем уведомление об успешном завершении")

    try:
        # Получаем финальный отчет
        final_report = context['task_instance'].xcom_pull(key='final_report', task_ids='generate_report')

        # Формируем сообщение
        subject = "Пакетная обработка данных завершена успешно"
        body = f"""
        <html>
        <body>
        <h2>Пакетная обработка данных завершена успешно!</h2>
        
        <p><strong>Время завершения:</strong> {final_report['timestamp']}</p>
        <p><strong>Обработано записей:</strong> {final_report['analysis_summary']['total_records']}</p>
        </body>
        </html>
        """

        # Получаем email адреса из конфигурации
        email_list = context['dag'].default_args.get('email', [os.getenv('EMAIL_ADMIN', 'admin@example.com')])

        # Отправляем email с fallback
        try:
            send_email(
                to_emails=email_list,
                subject=subject,
                html_content=body
            )
        except Exception as e:
            logging.warning("send_email failed")
        return True

    except Exception as e:
        logging.error("Ошибка при отправке уведомления: %s", str(e))
        raise

def send_failure_notification(**context):
    """
    Отправляет уведомление о неудачном завершении.
    """
    logging.error("Отправляем уведомление о неудачном завершении")

    try:
        # Получаем информацию об ошибке
        task_instance = context['task_instance']
        dag_run = context.get('dag_run')

        subject = "Ошибка в пакетной обработке данных"
        body = f"""
        <html>
        <body>
        <h2 style="color: red;">Ошибка в пакетной обработке данных!</h2>
        
        <p><strong>Время ошибки:</strong> {datetime.now().isoformat()}</p>
        <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
        <p><strong>Задача:</strong> {task_instance.task_id}</p>
        <p><strong>Попытка:</strong> {task_instance.try_number}</p>
        <p><strong>Статус:</strong> {task_instance.state}</p>
        </body>
        </html>
        """

        # Получаем email адреса из конфигурации
        email_list = context['dag'].default_args.get('email', [os.getenv('EMAIL_ADMIN', 'admin@example.com')])

        # Отправляем email с fallback
        try:
            send_email(
                to_emails=email_list,
                subject=subject,
                html_content=body
            )
            logging.error("Email уведомление об ошибке отправлено через Airflow")
        except Exception as e:
            logging.warning("send_email failed")
        return True

    except Exception as e:
        logging.error("Ошибка при отправке уведомления об ошибке: %s", str(e))
        raise

# Определение задач
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_and_validate_data,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=2),
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=2),
)

def branch_function(**context):
    analysis_results = context['task_instance'].xcom_pull(key='analysis_results', task_ids='analyze_data')
    if analysis_results and analysis_results.get('needs_processing', False):
        return 'process_high_earn_employees'
    else:
        return 'process_regular_employees'

branch_task = BranchPythonOperator(
    task_id='branch_on_analysis',
    python_callable=branch_function,
    dag=dag,
)

process_high_earn_employees_task = PythonOperator(
    task_id='process_high_earn_employees',
    python_callable=process_high_earn_employees,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=3),
)

process_regular_employees_task = PythonOperator(
    task_id='process_regular_employees',
    python_callable=process_regular_employees,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=3),
)

# Задача для объединения ветвей
join_task = EmptyOperator(
    task_id='join_branches',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_final_report,
    dag=dag,
    retries=1,
    retry_delay=timedelta(minutes=5),
)

# Задача для очистки временных файлов
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command=f'find {OUTPUT_DIR} -name "*.tmp" -delete 2>/dev/null || true',
    dag=dag,
    retries=1,
)

# Задача успешного завершения
success_notification_task = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

# Задача уведомления об ошибке
failure_notification_task = PythonOperator(
    task_id='failure_notification',
    python_callable=send_failure_notification,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

# Определение зависимостей между задачами
start_task >> read_data_task >> analyze_data_task >> branch_task

# Ветвление на основе анализа
branch_task >> process_high_earn_employees_task
branch_task >> process_regular_employees_task

# Объединение ветвей
process_high_earn_employees_task >> join_task
process_regular_employees_task >> join_task

# Финальная обработка
join_task >> generate_report_task >> cleanup_task

# Уведомления
cleanup_task >> [success_notification_task, failure_notification_task] >> end_task
