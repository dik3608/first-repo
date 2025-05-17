"""Slot Corpus Builder | v1.0 | © 2025

This script builds a small text corpus from search engine results using
archived snapshots from the Wayback Machine.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import requests
from langdetect import detect
from tqdm import tqdm
import aiohttp
import trafilatura


BANNER = "Slot Corpus Builder | v1.0 | © 2025"
LOG_FILE = "slot_corpus_builder.log"
SUPPORTED_LANGS = {"en", "ru", "es"}

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger.addHandler(logging.StreamHandler())


def read_queries(path: Path) -> List[str]:
    """Read search queries from Excel file.

    Args:
        path: Path to Excel file.

    Returns:
        List of queries.
    """
    df = pd.read_excel(path, sheet_name="Queries")
    return df["query"].dropna().tolist()


def bing_search(query: str, max_links: int = 10) -> List[str]:
    """Fetch URLs from Bing search results.

    Args:
        query: Search query.
        max_links: Maximum number of links to return.

    Returns:
        List of URLs.
    """
    params = {"q": query, "count": str(max_links)}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get("https://www.bing.com/search", params=params, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Search failed for %s: %s", query, exc)
        return []

    urls = []
    for part in resp.text.split("<a href="):
        if part.startswith("\""):
            url = part.split("\"")[1]
            if url.startswith("http") and "bing" not in url:
                urls.append(url)
        if len(urls) >= max_links:
            break
    return urls


def parse_cdx(json_data: list) -> Optional[Dict[str, str]]:
    """Parse Wayback Machine CDX API JSON data."""
    if len(json_data) < 2:
        return None
    timestamp, original = json_data[1]
    return {"timestamp": timestamp, "original": original}


def format_wayback_url(timestamp: str, original: str) -> str:
    return f"https://web.archive.org/web/{timestamp}/{original}"


def snapshot_date(timestamp: str) -> str:
    dt = datetime.strptime(timestamp[:8], "%Y%m%d")
    return dt.strftime("%Y-%m-%d")


async def fetch_snapshot(session: aiohttp.ClientSession, url: str, until: str) -> Optional[Dict[str, str]]:
    """Fetch snapshot info from Wayback Machine."""
    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        f"?url={url}&to={until.replace('-', '')}&output=json&fl=timestamp,original&filter=statuscode:200&limit=1"
    )
    try:
        async with session.get(cdx_url, timeout=10) as resp:
            text = await resp.text()
            data = json.loads(text)
            info = parse_cdx(data)
            if info:
                info["wayback_url"] = format_wayback_url(info["timestamp"], info["original"])
                info["snapshot_date"] = snapshot_date(info["timestamp"])
                return info
    except (aiohttp.ClientError, json.JSONDecodeError, asyncio.TimeoutError) as exc:
        logger.warning("CDX request failed for %s: %s", url, exc)
    return None


async def download_html(session: aiohttp.ClientSession, wayback_url: str) -> Optional[str]:
    """Download HTML from Wayback Machine snapshot."""
    try:
        await asyncio.sleep(1)
        async with session.get(wayback_url, timeout=15) as resp:
            if resp.status != 200:
                logger.warning("Non-200 status for %s: %s", wayback_url, resp.status)
                return None
            html = await resp.text()
            if not html:
                logger.warning("Empty page: %s", wayback_url)
                return None
            if len(html.encode("utf-8")) > 1_000_000:
                logger.warning("HTML too large (>1MB) for %s", wayback_url)
                return None
            return html
    except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeDecodeError) as exc:
        logger.warning("Failed to download %s: %s", wayback_url, exc)
        return None


def extract_text(html: str) -> Optional[str]:
    """Extract clean text from HTML using trafilatura with readability fallback."""
    if len(html.encode("utf-8")) < 3_000_000:
        text = trafilatura.extract(html)
        if text:
            return text
    # Fallback to readability-lxml
    try:
        from readability import Document

        doc = Document(html)
        cleaned = doc.summary(html_partial=True)
        text = trafilatura.extract(cleaned)
        return text
    except Exception as exc:  # noqa: BLE001
        logger.warning("Readability failed: %s", exc)
        return None


def count_tokens(text: str) -> int:
    return len(text.split())


def save_records(output_path: Path, records: List[Dict[str, str]]) -> None:
    """Save records to Excel file."""
    df_new = pd.DataFrame(records)
    if output_path.exists():
        df_old = pd.read_excel(output_path, sheet_name="Corpus")
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_all.to_excel(writer, sheet_name="Corpus", index=False)


async def process_query(query: str, max_links: int, until: str) -> List[Dict[str, str]]:
    """Process a single search query."""
    urls = bing_search(query, max_links)
    records = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            info = await fetch_snapshot(session, url, until)
            if not info:
                continue
            html = await download_html(session, info["wayback_url"])
            if not html:
                continue
            text = extract_text(html)
            if not text:
                logger.warning("Extraction failed for %s", info["wayback_url"])
                continue
            try:
                lang = detect(text)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Language detection failed for %s: %s", url, exc)
                continue
            if lang not in SUPPORTED_LANGS:
                logger.info("Unsupported language (%s) for %s", lang, url)
                continue
            record = {
                "query": query,
                "url": url,
                "wayback_url": info["wayback_url"],
                "snapshot_date": info["snapshot_date"],
                "text": text,
                "tokens": count_tokens(text),
            }
            records.append(record)
    return records


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Slot Corpus Builder")
    parser.add_argument("--input", default="queries.xlsx", help="Input Excel file with queries")
    parser.add_argument("--output", default="corpus.xlsx", help="Output Excel corpus file")
    parser.add_argument("--since", default="1996-01-01", help="Start date (unused, for future use)")
    parser.add_argument("--until", default="2020-12-31", help="Fetch snapshots up to this date")
    parser.add_argument("--max-links", type=int, default=10, help="Max links per query")
    return parser.parse_args()


async def main() -> None:
    print(BANNER)
    setup_logging()
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    queries = read_queries(input_path)

    all_records = []
    for query in tqdm(queries, desc="Queries"):
        recs = await process_query(query, args.max_links, args.until)
        all_records.extend(recs)

    if all_records:
        save_records(output_path, all_records)
        logger.info("Saved %d records to %s", len(all_records), output_path)
    else:
        logger.info("No records to save")


if __name__ == "__main__":
    asyncio.run(main())
