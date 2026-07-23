from __future__ import annotations

import subprocess
import sys
import time
from typing import Dict

from config.logger import logger


# ============================================================
# Helper function
# ============================================================

def run_step(step_name: str, module_name: str) -> float:
    """
    Execute one pipeline step as a Python module.

    The function raises an exception when the child module fails,
    allowing the main pipeline to return a non-zero exit code to Airflow.
    """

    logger.info("")
    logger.info("=" * 70)
    logger.info("STARTING: %s", step_name)
    logger.info("=" * 70)

    start_time = time.time()

    try:
        completed_process = subprocess.run(
            [
                sys.executable,
                "-m",
                module_name,
            ],
            check=True,
        )

        elapsed = round(time.time() - start_time, 2)

        logger.info("%s completed successfully.", step_name)
        logger.info("Execution Time: %.2f seconds", elapsed)

        return elapsed

    except subprocess.CalledProcessError as error:
        elapsed = round(time.time() - start_time, 2)

        logger.error("%s FAILED", step_name)
        logger.error("Module: %s", module_name)
        logger.error("Execution Time: %.2f seconds", elapsed)
        logger.error("Exit code: %s", error.returncode)

        raise

    except Exception:
        elapsed = round(time.time() - start_time, 2)

        logger.exception(
            "Unexpected error during %s after %.2f seconds",
            step_name,
            elapsed,
        )

        raise


# ============================================================
# Pipeline summary
# ============================================================

def log_pipeline_summary(
    execution_summary: Dict[str, float],
    total_time: float,
) -> None:
    """Write the successful execution summary to the ETL log."""

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)

    for step_name, duration in execution_summary.items():
        logger.info(
            "%-40s %10.2f seconds",
            step_name,
            duration,
        )

    logger.info("-" * 70)
    logger.info(
        "%-40s %10.2f seconds",
        "TOTAL LOCAL PIPELINE",
        total_time,
    )
    logger.info("=" * 70)


# ============================================================
# Main local pipeline
# ============================================================

def main() -> int:
    """
    Run the local portion of the AdventureWorks pipeline.

    Local stages:
    1. Extract, validate, and download files from AWS S3.
    2. Upload the validated files to the Databricks Volume.

    Databricks Bronze, Silver, Gold, and SQL Analytics jobs are
    orchestrated separately by the Airflow DAG.
    """

    pipeline_start = time.time()
    execution_summary: Dict[str, float] = {}

    logger.info("")
    logger.info("=" * 70)
    logger.info("ADVENTUREWORKS RETAIL CLOUD ETL PIPELINE")
    logger.info("=" * 70)
    logger.info("Local pipeline started")
    logger.info("")

    try:
        # ----------------------------------------------------
        # Step 1: AWS S3 extraction and validation
        # ----------------------------------------------------

        execution_summary["AWS S3 extraction and validation"] = run_step(
            step_name="AWS S3 extraction and validation",
            module_name="scripts.s3_extractor",
        )

        # ----------------------------------------------------
        # Step 2: Upload validated CSV files to Databricks
        # ----------------------------------------------------

        execution_summary["Upload to Databricks Volume"] = run_step(
            step_name="Upload to Databricks Volume",
            module_name="scripts.upload_to_databricks_volume",
        )

    except subprocess.CalledProcessError as error:
        total_time = round(time.time() - pipeline_start, 2)

        logger.error("")
        logger.error("=" * 70)
        logger.error("LOCAL PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error("Total elapsed time: %.2f seconds", total_time)
        logger.error(
            "A pipeline module returned exit code %s.",
            error.returncode,
        )
        logger.error(
            "Databricks jobs will not be triggered because the "
            "Airflow extraction task will fail."
        )
        logger.error("=" * 70)

        return error.returncode or 1

    except Exception:
        total_time = round(time.time() - pipeline_start, 2)

        logger.exception(
            "Local pipeline stopped unexpectedly after %.2f seconds.",
            total_time,
        )

        return 1

    total_time = round(time.time() - pipeline_start, 2)

    log_pipeline_summary(
        execution_summary=execution_summary,
        total_time=total_time,
    )

    logger.info("")
    logger.info(
        "Local pipeline completed successfully. "
        "Validated files are available in the Databricks Volume."
    )
    logger.info("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
