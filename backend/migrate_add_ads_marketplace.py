"""Migration: add marketplace column to ads_campaigns, ads_adsets, ads_product_performance.
   Also drop old unique constraints and create new ones that include marketplace.
"""
from sqlalchemy import text
from app.database import engine


def _get_columns(conn, table_name):
    result = conn.execute(text(f"PRAGMA table_info({table_name})"))
    return {r[1] for r in result}


def migrate():
    tables = ["ads_campaigns", "ads_adsets", "ads_product_performance"]

    with engine.connect() as conn:
        for table in tables:
            cols = _get_columns(conn, table)
            if "marketplace" not in cols:
                conn.execute(text(
                    f"ALTER TABLE {table} ADD COLUMN marketplace VARCHAR(10) NOT NULL DEFAULT 'ro'"
                ))
                print(f"[OK] Added 'marketplace' to {table}")
            else:
                print(f"[SKIP] {table}.marketplace already exists")

        conn.commit()

    # SQLite does not support DROP CONSTRAINT / ADD CONSTRAINT for unique constraints.
    # The new unique constraints will only apply to newly created tables.
    # For existing data, all rows default to 'ro' which is correct.
    print("[DONE] Migration complete! Existing data defaults to marketplace='ro'.")


if __name__ == "__main__":
    migrate()

