#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

USER_AGENT = "TempoPerso-RankingBot/1.0 (+https://github.com/)"
TIMEOUT = 30
MAX_PAGES_DEFAULT = 5

CATEGORY_CONFIG = [
    {"key": "all", "label": "Libri", "path": "classifica/libri"},
    {"key": "narrativa-italiana", "label": "Narrativa italiana", "path": "classifica/libri_narrativa-italiana"},
    {"key": "narrativa-straniera", "label": "Narrativa straniera", "path": "classifica/libri_narrativa-straniera"},
    {"key": "bambini-ragazzi", "label": "Bambini e ragazzi", "path": "classifica/libri_bambini-ragazzi"},
    {"key": "religione-spiritualita", "label": "Religione e spiritualità", "path": "classifica/libri_religione-spiritualita"},
]
PERIODS = [
    {"key": "1week", "label": "1week"},
    {"key": "1month", "label": "1month"},
    {"key": "1year", "label": "1year"},
]

HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "it-IT,it;q=0.9,en;q=0.6"}

@dataclass
class RankingRow:
    ean: str = ""
    title: str = ""
    author: str = ""
    position: int = 999999
    category: str = ""
    period: str = ""
    source: str = "IBS"
    url: str = ""
    cover: str = ""
    publisher: str = ""
    year: str = ""
    fetched_at: str = ""

    def key(self) -> tuple:
        return (self.period, self.category, self.position, normalize_text(self.title), normalize_text(self.author), self.ean)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def build_page_url(category_path: str, period_key: str, page: int) -> str:
    # IBS accepts both SOLD and sold on public ranking pages.
    return f"https://www.ibs.it/{category_path}/{period_key}/SOLD?defaultPage={page}"


def fetch(url: str, session: requests.Session) -> str:
    r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def json_loads_safe(raw: str) -> Optional[Any]:
    try:
        return json.loads(raw)
    except Exception:
        return None


def walk_json(node: Any) -> Iterable[Any]:
    yield node
    if isinstance(node, dict):
        for v in node.values():
            yield from walk_json(v)
    elif isinstance(node, list):
        for v in node:
            yield from walk_json(v)


def extract_author(obj: Any) -> str:
    if isinstance(obj, str):
        return clean_spaces(obj)
    if isinstance(obj, list):
        names = [extract_author(x) for x in obj]
        names = [x for x in names if x]
        return ", ".join(names[:3])
    if isinstance(obj, dict):
        for key in ("name", "author", "title"):
            if obj.get(key):
                return extract_author(obj.get(key))
    return ""


def extract_ean_from_url(url: str) -> str:
    m = re.search(r"/e/(97[89]\d{10}|\d{9}[\dXx])(?:[/?#]|$)", url or "")
    return m.group(1).upper() if m else ""


def find_jsonld_rows(html: str, page_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        data = json_loads_safe(script.string or script.text or "")
        if not data:
            continue
        for node in walk_json(data):
            if isinstance(node, dict) and node.get("@type") == "ItemList" and isinstance(node.get("itemListElement"), list):
                for el in node["itemListElement"]:
                    if not isinstance(el, dict):
                        continue
                    pos = el.get("position") or el.get("item", {}).get("position")
                    item = el.get("item") if isinstance(el.get("item"), dict) else {}
                    url = item.get("url") or el.get("url") or ""
                    title = item.get("name") or el.get("name") or ""
                    author = extract_author(item.get("author") or el.get("author") or "")
                    cover = item.get("image") or ""
                    if title or url:
                        rows.append({
                            "position": int(pos) if str(pos).isdigit() else None,
                            "title": clean_spaces(title),
                            "author": clean_spaces(author),
                            "url": urljoin(page_url, url) if url else "",
                            "cover": cover,
                            "ean": extract_ean_from_url(url),
                        })
    return rows


def find_dom_rows(html: str, page_url: str) -> List[Dict[str, Any]]:
    # Fallback if JSON-LD is absent or incomplete. It parses visible ranking text blocks.
    text = BeautifulSoup(html, "lxml").get_text("\n", strip=True)
    lines = [clean_spaces(x) for x in text.splitlines() if clean_spaces(x)]
    rows: List[Dict[str, Any]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^(\d{1,3})°$", line)
        if not m:
            i += 1
            continue
        position = int(m.group(1))
        title = lines[i + 1] if i + 1 < len(lines) else ""
        author = ""
        for j in range(i + 2, min(i + 8, len(lines))):
            if lines[j].lower().startswith("di "):
                author = clean_spaces(lines[j][3:])
                break
        if title:
            rows.append({"position": position, "title": title, "author": author, "url": "", "cover": "", "ean": ""})
        i += 1
    # No URLs in this fallback; rows may still match your inventory by title+author.
    return rows


def parse_product_metadata(html: str, url: str) -> Dict[str, str]:
    meta = {"ean": extract_ean_from_url(url), "title": "", "author": "", "cover": "", "publisher": "", "year": ""}
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        data = json_loads_safe(script.string or script.text or "")
        if not data:
            continue
        for node in walk_json(data):
            if not isinstance(node, dict):
                continue
            if node.get("@type") in {"Book", "Product"}:
                meta["title"] = meta["title"] or clean_spaces(node.get("name") or "")
                meta["author"] = meta["author"] or clean_spaces(extract_author(node.get("author") or ""))
                meta["cover"] = meta["cover"] or clean_spaces(node.get("image") or "")
                meta["ean"] = meta["ean"] or clean_spaces(node.get("gtin13") or node.get("gtin") or node.get("isbn") or "")
                brand = node.get("brand") or {}
                if isinstance(brand, dict):
                    meta["publisher"] = meta["publisher"] or clean_spaces(brand.get("name") or "")
    page_text = soup.get_text("\n", strip=True)
    if not meta["ean"]:
        m = re.search(r"\bEAN\s*:?\s*(97[89]\d{10})\b", page_text, flags=re.I)
        if m:
            meta["ean"] = m.group(1)
    if not meta["year"]:
        m = re.search(r"\b(19\d{2}|20\d{2})\b", page_text)
        if m:
            meta["year"] = m.group(1)
    return meta


def unique_rows(rows: Iterable[RankingRow]) -> List[RankingRow]:
    best: Dict[tuple, RankingRow] = {}
    for row in rows:
        key = row.key()
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        # Prefer rows carrying EAN and cover metadata.
        if (not current.ean and row.ean) or (not current.cover and row.cover):
            best[key] = row
    return sorted(best.values(), key=lambda r: (r.period, r.category, r.position, normalize_text(r.title)))


def scrape_category_period(session: requests.Session, category: Dict[str, str], period: Dict[str, str], max_pages: int) -> List[RankingRow]:
    out: List[RankingRow] = []
    for page in range(1, max_pages + 1):
        url = build_page_url(category["path"], period["key"], page)
        try:
            html = fetch(url, session)
        except Exception as exc:
            print(f"WARN page fetch failed: {url} -> {exc}", file=sys.stderr)
            continue
        rows = find_jsonld_rows(html, url)
        if not rows:
            rows = find_dom_rows(html, url)
        if not rows:
            print(f"WARN no rows parsed: {url}", file=sys.stderr)
            continue
        page_count = 0
        for raw in rows:
            if not raw.get("title"):
                continue
            rr = RankingRow(
                ean=clean_spaces(raw.get("ean") or ""),
                title=clean_spaces(raw.get("title") or ""),
                author=clean_spaces(raw.get("author") or ""),
                position=int(raw.get("position") or 999999),
                category=category["label"],
                period=period["label"],
                source="IBS",
                url=raw.get("url") or "",
                cover=raw.get("cover") or "",
                fetched_at=now_iso(),
            )
            if rr.url and (not rr.ean or not rr.author):
                try:
                    meta = parse_product_metadata(fetch(rr.url, session), rr.url)
                    rr.ean = rr.ean or meta.get("ean", "")
                    rr.author = rr.author or meta.get("author", "")
                    rr.title = rr.title or meta.get("title", "")
                    rr.cover = rr.cover or meta.get("cover", "")
                    rr.publisher = meta.get("publisher", "")
                    rr.year = meta.get("year", "")
                    time.sleep(0.1)
                except Exception as exc:
                    print(f"WARN product fetch failed: {rr.url} -> {exc}", file=sys.stderr)
            out.append(rr)
            page_count += 1
        # Heuristic stop: if a page yielded very few rows, do not continue.
        if page_count < 8:
            break
    return out


def compute_score(position: int, period: str) -> float:
    base = max(0, 101 - int(position or 999999))
    weight = {"1week": 1.0, "1month": 0.8, "1year": 0.55}.get(period, 0.7)
    return round(base * weight, 2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape public bestseller rankings and emit classifiche.json")
    parser.add_argument("--output", default="classifiche.json")
    parser.add_argument("--max-pages", type=int, default=int(os.getenv("MAX_PAGES", MAX_PAGES_DEFAULT)))
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(HEADERS)

    rows: List[RankingRow] = []
    scrape_errors: List[str] = []
    for category in CATEGORY_CONFIG:
        for period in PERIODS:
            try:
                rows.extend(scrape_category_period(session, category, period, args.max_pages))
            except Exception as exc:
                scrape_errors.append(f"{category['key']}/{period['key']}: {exc}")
                print(f"WARN scrape failed for {category['key']} {period['key']}: {exc}", file=sys.stderr)

    rows = unique_rows(rows)
    payload = {
        "source": "IBS bestseller scraper",
        "updated_at": now_iso(),
        "generated_by": "scripts/update_classifiche.py",
        "settings": {
            "max_pages": args.max_pages,
            "categories": [c["label"] for c in CATEGORY_CONFIG],
            "periods": [p["label"] for p in PERIODS],
        },
        "errors": scrape_errors,
        "rankings": [
            {
                **{k: v for k, v in asdict(row).items() if v not in ("", None)},
                "score": compute_score(row.position, row.period),
            }
            for row in rows
        ],
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {len(rows)} rankings to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
