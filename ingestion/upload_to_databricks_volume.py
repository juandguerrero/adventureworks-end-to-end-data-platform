from __future__ import annotations

import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from config.logger import logger


LOCAL_DATA_DIRECTORY = Path("data/raw")

VOLUME_DIRECTORY = "/Volumes/workspace/default/adventureworks_raw"


def find_csv_files() -> list[Path]:
    csv_files = list(LOCAL_DATA_DIRECTORY.rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files were found inside {LOCAL_DATA_DIRECTORY.resolve()}"
        )

    return csv_files


def validate_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"Expected a file: {file_path}")

    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty: {file_path}")


def upload_file(
    workspace: WorkspaceClient,
    local_path: Path,
) -> None:
    validate_file(local_path)

    destination = f"{VOLUME_DIRECTORY}/{local_path.name}"

    logger.info("Uploading %s to %s", local_path, destination)

    with local_path.open("rb") as file_handle:
        workspace.files.upload(
            file_path=destination,
            contents=file_handle,
            overwrite=True,
        )

    logger.info("Uploaded successfully: %s", destination)


def main() -> int:
    try:
        logger.info("=" * 70)
        logger.info("UPLOADING CSV FILES TO DATABRICKS VOLUME")
        logger.info("=" * 70)

        workspace = WorkspaceClient()
        csv_files = find_csv_files()

        for csv_file in csv_files:
            upload_file(
                workspace=workspace,
                local_path=csv_file,
            )

        logger.info(
            "Upload completed successfully. Files uploaded: %s",
            len(csv_files),
        )

        return 0

    except Exception:
        logger.exception("Databricks Volume upload failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
