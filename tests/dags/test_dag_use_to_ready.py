# from airflow.models import DagBag


# def test_dag_is_ready_for_production():
#     """
#     운영에서 자주 발생하는 설정 실수를 CI에서 차단
#     현재는 의논 된 바가 없어 주석처리
#     """
#     dagbag = DagBag(
#         dag_folder="src/dags",
#         include_examples=False,
#     )

#     assert dagbag.import_errors == {}

#     for dag_id, dag in dagbag.dags.items():
#         # 기본 식별자
#         assert dag.dag_id == dag_id

#         # catchup 폭탄 방지
#         assert dag.catchup is False

#         # schedule 누락 방지
#         assert dag.schedule is not None, f"{dag_id} has no schedule"

#         # start_date 누락 방지
#         assert dag.start_date is not None, f"{dag_id} has no start_date"

#         # 빈 DAG 방지
#         assert len(dag.tasks) > 0, f"{dag_id} has no tasks"

#         # UI 운영성
#         assert dag.tags, f"{dag_id} has no tags"
