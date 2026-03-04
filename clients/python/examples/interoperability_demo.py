"""End-to-end interoperability demo for VeridicalDB handmade drivers.

Run Java demo first to seed/update rows, then run this Python demo.
"""

from pathlib import Path

import veridicaldb


def main() -> None:
    table_name = Path("/tmp/veridicaldb_interop_table.txt").read_text(encoding="utf-8").strip()
    conn = veridicaldb.connect(host="localhost", port=15432, database="default")
    try:
        cur = conn.cursor()

        cur.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
        rows = cur.fetchall()
        print("Rows visible from Python:")
        if not rows:
            print("  (no rows returned)")
        else:
            for row in rows:
                print(f"  id={row[0]}, name={row[1]}")

        # Persist another change from Python to validate Python->DB communication.
        cur.execute(f"UPDATE {table_name} SET name = ? WHERE id = ?", ("carol_python_updated", 3))
        conn.commit()

        cur.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
        rows = cur.fetchall()
        print("Rows after Python update:")
        if not rows:
            print("  (no rows returned)")
        else:
            for row in rows:
                print(f"  id={row[0]}, name={row[1]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
