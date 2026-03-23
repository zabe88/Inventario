#!/usr/bin/env python3
"""
Scraper classifiche libri MULTI-FONTE → Supabase ranking_charts
Versione 8: Libraccio + Mondadori Store + Giunti al Punto
Ogni fonte viene salvata separatamente, poi il DB calcola uno score incrociato.
Solo requests + BeautifulSoup, niente Playwright.
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}


def extract_isbn(url: str) -> str | None:
    """Estrai ISBN-13 da una URL."""
    m = re.search(r'(97[89]\d{10})', url or '')
    return m.group(1) if m else None


# ═══════════════════════════════════════════════════════
# PARSER: LIBRACCIO
# ═══════════════════════════════════════════════════════
def parse_libraccio(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    seen = set()
    pos = 1
    for link in soup.select('a[href*="/libro/"]'):
        href = link.get("href", "")
        if href in seen or "/autore/" in href:
            continue
        title = link.get_text(strip=True)
        if not title or len(title) < 4:
            continue
        seen.add(href)
        entry = {"position": pos, "title": title[:200]}
        ean = extract_isbn(href)
        if ean:
            entry["ean"] = ean
        parent = link.find_parent(["div", "li", "td"])
        if parent:
            auth = parent.select_one('a[href*="/autore/"]')
            if auth:
                entry["author"] = auth.get_text(strip=True)[:100]
        entries.append(entry)
        pos += 1
    return entries


# ═══════════════════════════════════════════════════════
# PARSER: GIUNTI AL PUNTO (Shopify-based)
# ═══════════════════════════════════════════════════════
def parse_giunti(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    seen = set()
    pos = 1
    # Giunti usa Shopify: prodotti in card con link /products/
    for link in soup.select('a[href*="/products/"]'):
        href = link.get("href", "")
        if href in seen:
            continue
        title = link.get_text(strip=True)
        if not title or len(title) < 4:
            continue
        # Salta link di navigazione
        if title.lower() in ("vedi tutto", "scopri", "aggiungi"):
            continue
        seen.add(href)
        entry = {"position": pos, "title": title[:200]}
        ean = extract_isbn(href)
        if ean:
            entry["ean"] = ean
        # Cerca autore vicino
        parent = link.find_parent(["div", "li", "article"])
        if parent:
            # Giunti mette l'autore in un elemento separato
            for el in parent.find_all(string=True):
                text = el.strip()
                if text and text != title and len(text) > 3 and len(text) < 80:
                    if not text.startswith("€") and not text.startswith("Aggiungi"):
                        entry["author"] = text[:100]
                        break
        entries.append(entry)
        pos += 1
    return entries


# ═══════════════════════════════════════════════════════
# PARSER: MONDADORI STORE
# ═══════════════════════════════════════════════════════
def parse_mondadori(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    seen = set()
    pos = 1
    # Mondadori Store usa link con ISBN o /p/
    for link in soup.select('a[href]'):
        href = link.get("href", "")
        ean = extract_isbn(href)
        if not ean or ean in seen:
            continue
        title = link.get_text(strip=True)
        if not title or len(title) < 4:
            continue
        seen.add(ean)
        entry = {"position": pos, "title": title[:200], "ean": ean}
        parent = link.find_parent(["div", "li", "article"])
        if parent:
            # Cerca testo autore
            for el in parent.select('.author, [class*="author"], [class*="brand"]'):
                author_text = el.get_text(strip=True)
                if author_text:
                    entry["author"] = author_text[:100]
                    break
        entries.append(entry)
        pos += 1
    return entries


# ═══════════════════════════════════════════════════════
# CONFIGURAZIONE CHART
# ═══════════════════════════════════════════════════════
CHARTS = [
    # LIBRACCIO
    {"name": "Libraccio Top 100", "category": "generale", "source_key": "libraccio",
     "url": "https://www.libraccio.it/Top100.aspx", "parser": parse_libraccio},
    {"name": "Libraccio Narrativa", "category": "narrativa", "source_key": "libraccio",
     "url": "https://www.libraccio.it/reparto/32/narrativa.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Ragazzi", "category": "bambini_ragazzi", "source_key": "libraccio",
     "url": "https://www.libraccio.it/reparto/27/libri-per-ragazzi.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Fumetti", "category": "fumetti", "source_key": "libraccio",
     "url": "https://www.libraccio.it/reparto/21/fumetti-e-graphic-novels.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Religione", "category": "religione", "source_key": "libraccio",
     "url": "https://www.libraccio.it/reparto/35/religione.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    # GIUNTI AL PUNTO
    {"name": "Giunti Top 20", "category": "generale", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/classifica-gap", "parser": parse_giunti},
    {"name": "Giunti Gialli Thriller", "category": "gialli_thriller", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/gialli-e-thriller", "parser": parse_giunti},
    {"name": "Giunti Bambini Ragazzi", "category": "bambini_ragazzi", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/bambini-e-ragazzi", "parser": parse_giunti},
    {"name": "Giunti TikTok", "category": "social_trend", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/i-piu-amati-su-tik-tok", "parser": parse_giunti},
]


def scrape_chart(chart: dict) -> list[dict]:
    url = chart["url"]
    print(f"  Fetching {url} ...")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ⚠ Errore: {e}")
        return []
    return chart["parser"](resp.text)


def push_to_supabase(supabase_url, supabase_key, chart, entries, chart_date):
    rpc_url = f"{supabase_url}/rest/v1/rpc/ingest_ranking_chart"
    payload = {
        "p_source_key": chart["source_key"],
        "p_chart_name": chart["name"],
        "p_chart_category": chart["category"],
        "p_period": "weekly",
        "p_chart_date": chart_date,
        "p_entries": entries,
    }
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    resp = requests.post(rpc_url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def save_json(all_entries, output_path):
    feed = {
        "source": "Multi-source rankings v8",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rankings": [{
            "ean": e.get("ean", ""),
            "title": e.get("title", ""),
            "author": e.get("author", ""),
            "position": e.get("position", 0),
            "category": e.get("_category", ""),
            "source": e.get("_source", ""),
            "period": "1week",
        } for e in all_entries]
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON salvato: {output_path} ({len(feed['rankings'])} entries)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supabase-url")
    parser.add_argument("--supabase-key")
    parser.add_argument("--output", default="classifiche.json")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_entries = []
    total_ingested = 0
    total_matched = 0

    print(f"=== Multi-source rankings v8 — {chart_date} ===")
    print(f"    Fonti: Libraccio, Giunti al Punto")
    print(f"    Classifiche: {len(CHARTS)}\n")

    for chart in CHARTS:
        print(f"\n📊 [{chart['source_key'].upper()}] {chart['name']} ({chart['category']})")
        entries = scrape_chart(chart)
        print(f"   Trovati {len(entries)} titoli")

        if not entries:
            continue

        for e in entries:
            e["_category"] = chart["category"]
            e["_source"] = chart["source_key"]
        all_entries.extend(entries)

        if args.supabase_url and args.supabase_key and not args.json_only:
            try:
                clean = [{k: v for k, v in e.items() if not k.startswith("_")} for e in entries]
                result = push_to_supabase(
                    args.supabase_url, args.supabase_key,
                    chart, clean, chart_date
                )
                if isinstance(result, list) and len(result) > 0:
                    r = result[0]
                    total_ingested += r.get("ingested", 0)
                    total_matched += r.get("matched", 0)
                    print(f"   → DB: {r.get('ingested',0)} ingested, {r.get('matched',0)} matched")
                else:
                    print(f"   → DB: ok")
            except Exception as e:
                print(f"   ⚠ Errore Supabase: {e}")

        time.sleep(1.5)

    save_json(all_entries, args.output)

    print(f"\n{'='*50}")
    print(f"Totale: {len(all_entries)} titoli da {len(CHARTS)} classifiche")
    if total_ingested > 0:
        print(f"Supabase: {total_ingested} ingested, {total_matched} matched")

    # Riepilogo per fonte
    from collections import Counter
    by_source = Counter(e["_source"] for e in all_entries)
    for src, cnt in by_source.most_common():
        print(f"  {src}: {cnt} titoli")

    print("Done!")


if __name__ == "__main__":
    main()
