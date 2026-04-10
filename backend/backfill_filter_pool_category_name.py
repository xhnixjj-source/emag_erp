"""
补充筛选池 filter_pool.category_name

数据来源（与爬虫写入逻辑一致，见 app.services.crawler）：
1. keyword_links.category：同一 product_url 取 crawled_at 最新的一条非空类目
2. 若仍为空且 filter_pool.category_url 存在：从 URL 路径解析 slug（extract_category_name_from_url）

用法（在 backend 目录下）：
  python backfill_filter_pool_category_name.py              # 只补空类目，写库
  python backfill_filter_pool_category_name.py --dry-run    # 只统计不写库
  python backfill_filter_pool_category_name.py --force      # 覆盖已有 category_name（仍以 keyword_link 优先）
  python backfill_filter_pool_category_name.py --limit 100  # 最多处理 100 条
"""
from __future__ import annotations

import argparse
from sqlalchemy import or_

from app.database import SessionLocal
from app.models.keyword import KeywordLink
from app.models.product import FilterPool
from app.services.product_info_service import extract_category_name_from_url


def main() -> None:
    parser = argparse.ArgumentParser(description="补充 filter_pool.category_name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印统计，不提交数据库",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="也更新已有非空 category_name（keyword_links 优先）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="最多处理多少条筛选池记录（用于试跑）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="每批按 product_url IN 查询 keyword_links 的批量大小",
    )
    args = parser.parse_args()

    with SessionLocal() as db:
        q = db.query(FilterPool)
        if not args.force:
            q = q.filter(
                or_(
                    FilterPool.category_name.is_(None),
                    FilterPool.category_name == "",
                )
            )
        rows = q.order_by(FilterPool.id.asc()).all()
        if args.limit is not None:
            rows = rows[: args.limit]

        if not rows:
            print("没有需要处理的筛选池记录。")
            return

        print(f"待处理记录数: {len(rows)}（force={'是' if args.force else '否'}）")

        from_links = 0
        from_url = 0
        unchanged = 0
        batch_size = max(50, args.batch_size)

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            urls = list({fp.product_url for fp in batch if fp.product_url})
            url_to_category: dict[str, str] = {}
            if urls:
                links = (
                    db.query(KeywordLink)
                    .filter(
                        KeywordLink.product_url.in_(urls),
                        KeywordLink.category.isnot(None),
                        KeywordLink.category != "",
                    )
                    .order_by(KeywordLink.crawled_at.desc())
                    .all()
                )
                for link in links:
                    pu = link.product_url
                    if pu in url_to_category:
                        continue
                    c = (link.category or "").strip()
                    if c:
                        url_to_category[pu] = c

            for fp in batch:
                if not args.force and fp.category_name:
                    unchanged += 1
                    continue

                new_cat: str | None = None
                if fp.product_url and fp.product_url in url_to_category:
                    new_cat = url_to_category[fp.product_url]
                elif fp.category_url:
                    new_cat = extract_category_name_from_url(fp.category_url)

                if not new_cat:
                    unchanged += 1
                    continue

                if fp.category_name == new_cat:
                    unchanged += 1
                    continue

                if fp.product_url and fp.product_url in url_to_category:
                    from_links += 1
                else:
                    from_url += 1

                if not args.dry_run:
                    fp.category_name = new_cat

            if not args.dry_run:
                db.commit()

        print(
            "完成。"
            f" 从 keyword_links 更新: {from_links}；"
            f" 从 category_url 解析: {from_url}；"
            f" 未变更/无来源: {unchanged}。"
        )
        if args.dry_run:
            print("（dry-run：未写入数据库，上述变更未持久化）")
            db.rollback()


if __name__ == "__main__":
    main()
