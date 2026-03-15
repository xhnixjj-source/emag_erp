"""Migration: add part_number and part_number_key columns to ads_product_performance"""
from sqlalchemy import text
from app.database import engine


def migrate():
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(ads_product_performance)"))
        existing_cols = {r[1] for r in result}

        if "part_number" not in existing_cols:
            conn.execute(text("ALTER TABLE ads_product_performance ADD COLUMN part_number VARCHAR(255)"))
            print("[OK] Added column: part_number")
        else:
            print("[SKIP] Column part_number already exists")

        if "part_number_key" not in existing_cols:
            conn.execute(text("ALTER TABLE ads_product_performance ADD COLUMN part_number_key VARCHAR(255)"))
            print("[OK] Added column: part_number_key")
        else:
            print("[SKIP] Column part_number_key already exists")

        conn.commit()
    print("[DONE] Migration complete!")


if __name__ == "__main__":
    migrate()
