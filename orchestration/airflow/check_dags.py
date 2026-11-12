"""CI check: the DAG folder imports cleanly and defines the expected DAG.

Parses the files directly instead of asking the metadata database, which in Airflow 3 only
knows about DAGs once a DAG processor has run.

    python check_dags.py /opt/airflow/dags
"""

import sys

try:  # Airflow 3.1+
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # Airflow 3.0
    from airflow.models.dagbag import DagBag

EXPECTED = {"bank_monthly_refresh"}


def main(folder: str) -> int:
    # Examples are off through AIRFLOW__CORE__LOAD_EXAMPLES; newer DagBags dropped the kwarg.
    bag = DagBag(dag_folder=folder)
    for path, error in bag.import_errors.items():
        print(f"IMPORT ERROR {path}:\n{error}")
    missing = EXPECTED - set(bag.dag_ids)
    if missing:
        print(f"Missing DAGs: {sorted(missing)}; found {sorted(bag.dag_ids)}")
    for dag_id in sorted(bag.dag_ids):
        print(f"OK {dag_id}: {len(bag.dags[dag_id].tasks)} tasks")
    return 1 if bag.import_errors or missing else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dags"))
