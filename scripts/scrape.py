#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import time
import random
from datetime import datetime, timezone

import pandas as pd
from google_play_scraper import reviews, Sort


def parse_args():
    p = argparse.ArgumentParser(
        description="Scrape Google Play reviews (incremental-capable) and save as CSV."
    )
    p.add_argument("--app-id", default="com.openai.chatgpt", help="Google Play app id")
    p.add_argument("--lang", default="en")
    p.add_argument("--country", default="us")
    p.add_argument("--max-reviews", type=int, default=5000, help="max rows to fetch this run")
    p.add_argument("--batch-size", type=int, default=200, help="rows per API call (<=200)")
    p.add_argument("--since", default=None, help="only keep reviews with at >= this date (YYYY-MM-DD)")
    p.add_argument("--out", default=None, help="output CSV path; default: data/raw_YYYYMMDD_HHMM.csv")
    return p.parse_args()


def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def standardize_rows(raw_rows, app_id):
    """Map library fields -> our staging schema columns."""
    std = []
    now_utc = pd.Timestamp.utcnow()

    def to_ntz(dt):
        if dt is None:
            return pd.NaT
        ts = pd.to_datetime(dt, errors="coerce")
        # 只有 tz-aware 才能 tz_convert；tz-naive 直接返回
        if getattr(ts, "tzinfo", None) is not None:
            return ts.tz_convert(None)
        return ts  # already tz-naive

    for r in raw_rows:
        std.append({
            "review_id": r.get("reviewId"),
            "review_text": r.get("content", ""),
            "score": r.get("score"),
            "thumbs_up_count": r.get("thumbsUpCount", 0),
            "app_id": app_id,
            "app_version": r.get("appVersion") or r.get("reviewCreatedVersion"),
            "user_name": r.get("userName"),
            "review_created_at": to_ntz(r.get("at")),  # ← 用安全转换
            "scraped_at": now_utc,
        })
    return std



def main():
    args = parse_args()

    # ------- logging -------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("scrape")

    # ------- since filter (incremental stop condition) -------
    since_dt = None
    if args.since:
        try:
            since_dt = pd.Timestamp(args.since).tz_localize("UTC").tz_convert(None)
        except Exception:
            log.error("Invalid --since format, use YYYY-MM-DD, e.g. 2025-08-10")
            sys.exit(2)

    # ------- output path -------
    if args.out:
        out_path = args.out
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        out_path = f"data/raw_{ts}.csv"
    ensure_dir(out_path)

    # ------- crawl loop -------
    target_total = int(args.max_reviews)
    batch_size = min(int(args.batch_size), 200)

    all_rows = []
    token = None
    fetched = 0
    page = 0

    log.info(
        f"Start scraping app_id={args.app_id}, lang={args.lang}, country={args.country}, "
        f"max_reviews={target_total}, batch_size={batch_size}, since={args.since}"
    )

    try:
        while fetched < target_total:
            page += 1
            raw, token = reviews(
                args.app_id,
                lang=args.lang,
                country=args.country,
                sort=Sort.NEWEST,      # 按最新排序，便于 since 早停
                count=batch_size,
                continuation_token=token
            )

            if not raw:
                log.info("No more reviews from API. Stop.")
                break

            # 如果设置了 since，只保留 >= since 的评论
            if since_dt is not None:
                raw = [r for r in raw if pd.to_datetime(r.get("at")) >= since_dt]

                # NEWEST 排序下，若这一页已经全部早于 since，可以提前停止
                if len(raw) == 0:
                    log.info("Current page reviews are all older than --since. Early stop.")
                    break

            # 追加并截断到 target_total
            can_take = min(len(raw), target_total - fetched)
            all_rows.extend(raw[:can_take])
            fetched += can_take

            log.info(f"Fetched page {page}, got {can_take}, total={fetched}")

            # API礼貌等待
            time.sleep(random.uniform(0.8, 1.6))

            # 没有更多翻页token也停止
            if token is None:
                log.info("No continuation_token. Stop.")
                break

        if len(all_rows) == 0:
            log.error("No rows collected. Nothing to write.")
            sys.exit(1)

        # 标准化列
        std_rows = standardize_rows(all_rows, args.app_id)
        df = pd.DataFrame(std_rows)

        # 基本类型校正（可避免后续入仓出问题）
        df["score"] = pd.to_numeric(df["score"], errors="coerce").astype("Int64")
        df["thumbs_up_count"] = pd.to_numeric(df["thumbs_up_count"], errors="coerce").fillna(0).astype("Int64")
        # 时间列转为无时区（Snowflake TIMESTAMP_NTZ）
        df["review_created_at"] = pd.to_datetime(df["review_created_at"], errors="coerce")
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

        # 写CSV
        df.to_csv(out_path, index=False)
        log.info(f"Wrote CSV: {out_path} rows={len(df)}")

    except Exception as e:
        log.exception("Scrape failed with exception:")
        sys.exit(1)

    log.info("Scrape done.")
    sys.exit(0)


if __name__ == "__main__":
    main()
