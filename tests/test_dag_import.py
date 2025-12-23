from airflow.models import DagBag


def test_dag_import_and_ready():
    """
    DAG가 에러 없이 import 되고
    Scheduler가 즉시 실행 가능한 상태인지 검증
    """
    dagbag = DagBag(
        dag_folder="src/dags",
        include_examples=False,
    )

    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"

    for dag_id, dag in dagbag.dags.items():
        assert dag.dag_id == dag_id
        assert dag.catchup is False
