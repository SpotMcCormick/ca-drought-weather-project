import logging
import subprocess
import sys
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

if __name__ == "__main__":
    try:
        with open(CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)

        raw_log_path = Path(config["logging"]["incremental_load_log_file"])
        log_file_path = raw_log_path if raw_log_path.is_absolute() else PROJECT_ROOT / raw_log_path
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)-8s %(message)s',
            handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()
            ]
        )

        steps = config.get("incremental_etl", {}).get("steps", [])
        if not steps:
            raise ValueError("No incremental_etl.steps found in config/config.yaml")

        logging.info("Incremental ETL started")

        for step in steps:
            step_path = BASE_DIR / step
            if not step_path.exists():
                raise FileNotFoundError(f"Step file not found: {step_path}")

            logging.info(f"START {step}")
            result = subprocess.run([sys.executable, str(step_path)])
            if result.returncode != 0:
                raise RuntimeError(f"{step} failed with exit code {result.returncode}")
            logging.info(f"DONE  {step}")

        logging.info("Incremental ETL finished")
    except Exception as e:
        logging.exception(f"ETL failed: {e}")
        sys.exit(1)
