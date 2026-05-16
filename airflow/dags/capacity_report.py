"""
Daily BGP capacity report: prefix counts, top peers, churn rate.
Queries ClickHouse and publishes summary to Elasticsearch.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'network-ops',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

CLICKHOUSE_URL = "http://clickhouse:8123"
ES_URL = "http://elasticsearch:9200"


def query_prefix_counts(**context):
    """Pull 24h prefix counts from ClickHouse."""
    query = """
        SELECT
            toStartOfHour(timestamp) AS hour,
            countIf(withdrawn = 0) AS active_prefixes,
            countIf(withdrawn = 1) AS withdrawn_prefixes
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    context['ti'].xcom_push(key='prefix_counts', value=data.get('data', []))
    print(f"Retrieved {len(data.get('data', []))} hourly buckets")


def query_top_peers(**context):
    """Find top 10 BGP peers by route count over 24h."""
    query = """
        SELECT
            peer_asn,
            peer_ip,
            count() AS route_count
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 24 HOUR
          AND withdrawn = 0
        GROUP BY peer_asn, peer_ip
        ORDER BY route_count DESC
        LIMIT 10
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    context['ti'].xcom_push(key='top_peers', value=data.get('data', []))
    print(f"Top peer: AS{data['data'][0]['peer_asn'] if data.get('data') else 'N/A'}")


def publish_report(**context):
    """Push capacity report summary to Elasticsearch."""
    ti = context['ti']
    prefix_counts = ti.xcom_pull(key='prefix_counts', task_ids='query_prefix_counts') or []
    top_peers = ti.xcom_pull(key='top_peers', task_ids='query_top_peers') or []

    report = {
        '@timestamp': datetime.utcnow().isoformat(),
        'report_type': 'capacity',
        'total_buckets': len(prefix_counts),
        'top_peers': top_peers[:5],
        'generated_by': 'airflow/capacity_report',
    }

    resp = requests.post(
        f"{ES_URL}/capacity-reports/_doc",
        json=report,
        headers={'Content-Type': 'application/json'},
        timeout=10,
    )
    resp.raise_for_status()
    print(f"Published capacity report: {resp.json().get('_id')}")


with DAG(
    'bgp_capacity_report',
    default_args=default_args,
    description='Daily BGP capacity report: prefix counts, churn, top peers',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bgp', 'capacity', 'observability'],
) as dag:

    t_prefix = PythonOperator(task_id='query_prefix_counts', python_callable=query_prefix_counts)
    t_peers = PythonOperator(task_id='query_top_peers', python_callable=query_top_peers)
    t_report = PythonOperator(task_id='publish_report', python_callable=publish_report)

    [t_prefix, t_peers] >> t_report
