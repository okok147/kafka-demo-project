import subprocess
import sys
import time


def run_forever() -> None:
    while True:
        try:
            subprocess.run([sys.executable, "/app/scripts/bootstrap_topics.py"], check=True)
        except Exception as exc:
            print(f"topic bootstrap failed: {exc}")

        try:
            subprocess.run([sys.executable, "/app/scripts/admin_bootstrap.py"], check=True)
        except Exception as exc:
            print(f"admin bootstrap failed: {exc}")

        time.sleep(60)


if __name__ == "__main__":
    run_forever()
