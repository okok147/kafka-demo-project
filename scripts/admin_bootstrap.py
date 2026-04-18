import os
import time

import psycopg2


def main() -> None:
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=os.environ.get("POSTGRES_HOST", "postgres"),
                port=os.environ.get("POSTGRES_PORT", "5432"),
                dbname=os.environ.get("POSTGRES_DB", "kafkademo"),
                user=os.environ.get("POSTGRES_USER", "kafka"),
                password=os.environ.get("POSTGRES_PASSWORD", "kafka"),
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO account_limits(account_id, max_notional, max_open_orders)
                        VALUES
                          ('ACC-001', 200000, 20),
                          ('ACC-002', 50000, 5)
                        ON CONFLICT (account_id)
                        DO UPDATE SET
                          max_notional = EXCLUDED.max_notional,
                          max_open_orders = EXCLUDED.max_open_orders,
                          updated_at = NOW();
                        """
                    )
            conn.close()
            print("admin bootstrap done")
            return
        except Exception as exc:  # pragma: no cover - infra bootstrap
            print(f"waiting postgres ({attempt}): {exc}")
            time.sleep(2)

    raise RuntimeError("failed admin bootstrap")


if __name__ == "__main__":
    main()
