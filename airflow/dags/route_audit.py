"""
Weekly BGP route audit: detect prefix churn anomalies, RPKI invalid origins,
and new prefixes not seen in the previous week.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'network-ops',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}

CLICKHOUSE_URL = "http://clickhouse:8123"
ES_URL = "http://elasticsearch:9200"
RPKI_API = "https://rpki.cloudflare.com/api/v1/validity"


def detect_churn_anomalies(**context):
    """Flag prefixes with > 10 state changes in 24h (flapping)."""
    query = """
        SELECT
            prefix,
            origin_as,
            count() AS change_count
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY prefix, origin_as
        HAVING change_count > 10
        ORDER BY change_count DESC
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    anomalies = resp.json().get('data', [])
    context['ti'].xcom_push(key='churn_anomalies', value=anomalies)
    print(f"Found {len(anomalies)} churn anomalies")
    return len(anomalies)


def detect_new_prefixes(**context):
    """Find prefixes seen this week but not in the previous week."""
    query = """
        SELECT DISTINCT prefix, origin_as
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 7 DAY
          AND withdrawn = 0
          AND prefix NOT IN (
            SELECT DISTINCT prefix FROM bgp_routes
            WHERE timestamp BETWEEN now() - INTERVAL 14 DAY AND now() - INTERVAL 7 DAY
          )
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    new_prefixes = resp.json().get('data', [])
    context['ti'].xcom_push(key='new_prefixes', value=new_prefixes)
    print(f"Found {len(new_prefixes)} new prefixes this week")


def validate_rpki(**context):
    """Spot-check top 20 active prefixes against RPKI."""
    query = """
        SELECT prefix, origin_as, count() AS routes
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 1 HOUR AND withdrawn = 0
        GROUP BY prefix, origin_as
        ORDER BY routes DESC
        LIMIT 20
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    prefixes = resp.json().get('data', [])

    invalid = []
    for row in prefixes:
        try:
            r = requests.get(
                RPKI_API,
                params={'asn': f"AS{row['origin_as']}", 'prefix': row['prefix']},
                timeout=5
            )
            status = r.json().get('response', {}).get('status', 'unknown')
            if status == 'invalid':
                invalid.append({'prefix': row['prefix'], 'asn': row['origin_as'], 'rpki': status})
        except Exception:
            pass

    context['ti'].xcom_push(key='rpki_invalid', value=invalid)
    print(f"RPKI invalid prefixes: {len(invalid)}")


def publish_audit(**context):
    """Push audit findings to Elasticsearch."""
    ti = context['ti']
    audit = {
        '@timestamp': datetime.utcnow().isoformat(),
        'report_type': 'route_audit',
        'churn_anomalies': ti.xcom_pull(key='churn_anomalies', task_ids='detect_churn') or [],
        'new_prefixes_count': len(ti.xcom_pull(key='new_prefixes', task_ids='detect_new_prefixes') or []),
        'rpki_invalid': ti.xcom_pull(key='rpki_invalid', task_ids='validate_rpki') or [],
        'generated_by': 'airflow/route_audit',
    }
    resp = requests.post(
        f"{ES_URL}/route-audits/_doc",
        json=audit,
        headers={'Content-Type': 'application/json'},
        timeout=10,
    )
    resp.raise_for_status()
    print(f"Published route audit: {resp.json().get('_id')}")


with DAG(
    'bgp_route_audit',
    default_args=default_args,
    description='Weekly BGP route audit: churn anomalies, new prefixes, RPKI validation',
    schedule_interval='0 8 * * 1',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bgp', 'audit', 'rpki', 'observability'],
) as dag:

    t_churn = PythonOperator(task_id='detect_churn', python_callable=detect_churn_anomalies)
    t_new = PythonOperator(task_id='detect_new_prefixes', python_callable=detect_new_prefixes)
    t_rpki = PythonOperator(task_id='validate_rpki', python_callable=validate_rpki)
    t_publish = PythonOperator(task_id='publish_audit', python_callable=publish_audit)

    [t_churn, t_new, t_rpki] >> t_publish
