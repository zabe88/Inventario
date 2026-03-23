#!/usr/bin/env python3
"""
Scraper classifiche IBS → Supabase ranking_charts
Versione 4: usa Playwright (browser headless) perché IBS carica via JS.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import requests

CHARTS = [
    {"name": "Top libri", "category": "generale",
     "url": "https://www.ibs.it/classifica/libri/1week/sold"},
    {"name": "Narrativa italiana", "category": "narrativa_italiana",
     "url": "https://www.ibs.it/classifica/libri_narrativa-italiana/1week/sold"},
    {"name": "Narrativa straniera", "category": "narrativa_straniera",
     "url": "https://www.ibs.it/classifica/libri_narrativa-straniera/1week/sold"},
    {"name": "Bambini e ragazzi", "category": "bambini_ragazzi",
     "url": "https://www.ibs.it/classifica/libri_bambini-ragazzi/1week/sold"},
    {"name": "Religione e spiritualità", "category": "religione",
     "url": "https://www.ibs.it/classifica/libri_religione-spiritualita/1week/sold"},
    {"name": "Fumetti e graphic novel", "category": "fumetti",
     "url": "https://www.ibs.it/classifica/libri_fumetti-graphic-novels/1week/sold"},
    {"name": "Gialli e thriller", "category": "gialli_thriller",
     "url": "https://www.ibs.it/classifica/libri_gialli-thriller-horror/1week/sold"},
]


def extract_ean_from_url(url: str) -> str | None:
    m = re.search(r'/e/(\d{13})$', url or '')
    if m and m.group(1).startswith(('978', '979')):
        return m.group(1)
    return None


def scrape_chart_playwright(page, chart: dict) -> list[dict]:
    """Scrapa una classifica IBS usando Playwright."""
    entries = []
    url = chart["url"]
    print(f"  Navigating to {url} ...")

    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        # Aspetta che appaiano i prodotti
        page.wait_for_selector('a[href*="/e/"]', timeout=15000)
        time.sleep(2)  # Extra wait per rendering completo
    except Exception as e:
        print(f"  ⚠ Errore navigazione: {e}")
        return entries

    # Estrai tutti i link prodotto con /e/ (EAN a 13 cifre)
    links = page.query_selector_all('a[href*="/e/"]')
    seen = set()
    position = 1

    for link in links:
        href = link.get_attribute("href") or ""
        if "/e/" not in href or href in seen:
            continue

        # Filtra solo link che sembrano prodotti (non menu/sidebar)
        ean = extract_ean_from_url(href)
        title_text = link.inner_text().strip()

        # Salta link troppo corti o di navigazione
        if not title_text or len(title_text) < 3:
            continue
        if title_text.lower() in ("vedi tutti", "scopri", "iscriviti"):
            continue

        seen.add(href)
        entry = {"position": position, "title": title_text[:200]}
        if ean:
            entry["ean"] = ean

        # Prova a trovare autore nel contesto vicino
        parent = link.evaluate_handle("el => el.closest('div, li, article')")
        if parent:
            try:
                parent_text = parent.inner_text()
                # Cerca pattern "di NomeCognome"
                author_match = re.search(r'\bdi\s+([A-Z][a-zà-ú]+ [A-Z][a-zà-ú]+(?:\s[A-Z][a-zà-ú]+)?)', parent_text)
                if author_match:
                    entry["author"] = author_match.group(1).strip()[:100]
            except:
                pass

        entries.append(entry)
        position += 1

    return entries


def push_to_supabase(supabase_url, supabase_key, chart, entries, chart_date):
    rpc_url = f"{supabase_url}/rest/v1/rpc/ingest_ranking_chart"
    payload = {
        "p_source_key": "ibs",
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
        "source": "Scraper classifiche IBS v4 (Playwright)",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rankings": [{
            "ean": e.get("ean", ""),
            "title": e.get("title", ""),
            "author": e.get("author", ""),
            "position": e.get("position", 0),
            "category": e.get("_chart_category", ""),
            "period": "1week",
            "source": "IBS",
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

    # Import Playwright qui per non fallire se non installato
    from playwright.sync_api import sync_playwright

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_entries = []
    total_ingested = 0
    total_matched = 0

    print(f"=== Scraper classifiche IBS v4 (Playwright) — {chart_date} ===\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
            locale="it-IT",
        )
        page = context.new_page()

        for chart in CHARTS:
            print(f"\n📊 {chart['name']} ({chart['category']})")
            entries = scrape_chart_playwright(page, chart)
            print(f"   Trovati {len(entries)} titoli")

            if not entries:
                continue

            for e in entries:
                e["_chart_category"] = chart["category"]
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
                except Exception as e:
                    print(f"   ⚠ Errore Supabase: {e}")

            time.sleep(2)

        browser.close()

    save_json(all_entries, args.output)

    print(f"\n{'='*50}")
    print(f"Totale: {len(all_entries)} titoli da {len(CHARTS)} classifiche")
    if total_ingested > 0:
        print(f"Supabase: {total_ingested} ingested, {total_matched} matched")
    print("Done!")


if __name__ == "__main__":
    main()
