"""
Structural tests for the Prefect pipeline wiring.
Does NOT execute the pipeline against real data.
"""
import subprocess
import sys


def test_staging_flow_import():
    from src.pipeline.flows.staging_flow import staging_flow
    assert staging_flow is not None


def test_report_flow_import():
    from src.pipeline.flows.report_flow import report_flow
    assert report_flow is not None


def test_stage_tasks_import():
    from src.pipeline.tasks import stage_tasks
    assert hasattr(stage_tasks, "task_pub_to_stg")
    assert hasattr(stage_tasks, "task_raw_to_stg")
    assert hasattr(stage_tasks, "task_dv_cleaner")
    assert hasattr(stage_tasks, "task_tb_sts")


def test_cluster_tasks_import():
    from src.pipeline.tasks import cluster_tasks
    assert hasattr(cluster_tasks, "task_tollbooth_neighbours")
    assert hasattr(cluster_tasks, "task_map_tb_id")
    assert hasattr(cluster_tasks, "task_tb_imt_stretch_id_rel")
    assert hasattr(cluster_tasks, "task_tb_stretch_id_sts")


def test_report_tasks_import():
    from src.pipeline.tasks import report_tasks
    assert hasattr(report_tasks, "task_growth_rate_report")
    assert hasattr(report_tasks, "task_toll_update_date_report")
    assert hasattr(report_tasks, "task_manage_data")
    assert hasattr(report_tasks, "task_revenue")


def test_run_py_help():
    result = subprocess.run(
        [sys.executable, "src/pipeline/run.py", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--from-step" in result.stdout


def test_staging_flow_is_prefect_flow():
    from src.pipeline.flows.staging_flow import staging_flow
    assert staging_flow.name == "stage-year"


def test_report_flow_is_prefect_flow():
    from src.pipeline.flows.report_flow import report_flow
    assert report_flow.name == "reports"
