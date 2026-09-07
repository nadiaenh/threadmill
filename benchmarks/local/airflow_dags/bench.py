"""Twenty independently scheduled Python jobs, four execution slots."""
import time
from airflow.sdk import DAG, task, get_current_context

with DAG(
    dag_id="threadmill_benchmark",
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:
    @task(retries=0)
    def job():
        delay = float(get_current_context()["dag_run"].conf.get("delay", 0))
        if delay:
            time.sleep(delay)
        return int(get_current_context()["dag_run"].conf.get("value", 41)) + 1

    for index in range(20):
        job.override(task_id=f"job_{index:02d}")()
