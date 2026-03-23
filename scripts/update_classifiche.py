#!/usr/bin/env python3
"""
Scraper classifiche IBS → Supabase ranking_charts
Versione 2: scrive direttamente nel database via RPC.
Fallback: genera anche classifiche.json per l'app HTML.

Uso:
  python scripts/update_classifiche.py --supabase-url URL --supabase-key KEY
  python scripts/update_classifiche.py --output classifiche.json  # solo JSON, no DB
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ── Configurazione classifiche IBS ──────────────────────────────
CHARTS = [
    {
        "name": "Top libri",
        "category": "generale",
        "url": "https://www.ibs.it/classifica-libri-piu-venduti-oggi-libri/e/lm010"
    },
    {
        "name": "Narrativa italiana",
        "category": "narrativa_italiana",
        "url": "https://www.ibs.it/classifica-narrativa-italiana-libri/e/lm020"
    },
    {
        "name": "Narrativa straniera",
        "category": "narrativa_straniera",
        "url": "https://www.ibs.it/classifica-narrativa-straniera-libri/e/lm030"
    },
    {
        "name": "Bambini e ragazzi",
        "category": "bambini_ragazzi",
        "url": "https://www.ibs.it/classifica-libri-bambini-ragazzi/e/lm040"
    },
    {
        "name": "Religione e spiritualità",
        "category": "religione",
        "url": "https://www.ibs.it/classifica-religione-e-spiritualita-libri/e/lm130"
    },
    {
        "name": "Fumetti e graphic novel",
        "category": "fumetti",
        "url": "https://www.ibs.it/classifica-fumetti-graphic-novel-libri/e/lm170"
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
}


def scrape_chart(chart: dict, max_pages: int = 3) -> list[dict]:
    """Scrapa una classifica IBS e restituisce lista di entries."""
    entries = []
    position = 1

    for page in range(1, max_pages + 1):
        url = chart["url"] if page == 1 else f"{chart['url']}?page={page}"
        print(f"  Fetching {url} ...")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"  ⚠ Errore fetch pagina {page}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # IBS usa div.cc-product-list-item o simili
        items = soup.select("[data-product-ean], .cc-product-list-item, .product-item")

        if not items:
            # Prova selettore alternativo
            items = soup.select("li.product-item, div.product-item")

        if not items:
            print(f"  ⚠ Nessun prodotto trovato a pagina {page}, stop.")
            break

        for item in items:
            entry = {"position": position}

            # EAN
            ean = item.get("data-product-ean", "")
            if not ean:
                ean_el = item.select_one("[data-ean]")
                if ean_el:
                    ean = ean_el.get("data-ean", "")
            if ean and re.match(r"^97[89]\d{10}$", ean.strip()):
                entry["ean"] = ean.strip()

            # Titolo
            title_el = (
                item.select_one("h2.cc-product-title, .product-title, h3 a, h2 a")
            )
            if title_el:
                entry["title"] = title_el.get_text(strip=True)
            else:
                entry["title"] = f"Posizione {position}"

            # Autore
            author_el = item.select_one(
                ".cc-product-author, .product-author, .author"
            )
            if author_el:
                entry["author"] = author_el.get_text(strip=True)

            # Editore
            pub_el = item.select_one(
                ".cc-product-publisher, .product-publisher, .publisher"
            )
            if pub_el:
                entry["publisher"] = pub_el.get_text(strip=True)

            entries.append(entry)
            position += 1

        time.sleep(1.5)  # Rispetto rate limit

    return entries


def push_to_supabase(
    supabase_url: str, supabase_key: str,
    chart: dict, entries: list[dict], chart_date: str
) -> dict:
    """Invia entries a Supabase via RPC ingest_ranking_chart."""
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


def save_json(all_entries: list[dict], output_path: str, chart_date: str):
    """Salva il feed JSON per l'app HTML (retrocompatibilità)."""
    feed = {
        "source": "Scraper classifiche IBS v2",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rankings": []
    }
    for e in all_entries:
        feed["rankings"].append({
            "ean": e.get("ean", ""),
            "title": e.get("title", ""),
            "author": e.get("author", ""),
            "position": e.get("position", 0),
            "category": e.get("_chart_category", ""),
            "period": "1week",
            "source": "IBS",
        })
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    print(f"\n✓ JSON salvato: {output_path} ({len(feed['rankings'])} entries)")


def main():
    parser = argparse.ArgumentParser(description="Scraper classifiche IBS")
    parser.add_argument("--supabase-url", help="URL progetto Supabase")
    parser.add_argument("--supabase-key", help="Supabase anon/service key")
    parser.add_argument("--output", default="classifiche.json", help="Output JSON path")
    parser.add_argument("--max-pages", type=int, default=3, help="Pagine per classifica")
    parser.add_argument("--json-only", action="store_true", help="Solo JSON, no Supabase")
    args = parser.parse_args()

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_entries = []
    total_ingested = 0
    total_matched = 0

    print(f"=== Scraper classifiche IBS — {chart_date} ===\n")

    for chart in CHARTS:
        print(f"\n📊 {chart['name']} ({chart['category']})")
        entries = scrape_chart(chart, max_pages=args.max_pages)
        print(f"   Trovati {len(entries)} titoli")

        if not entries:
            continue

        # Aggiungi metadata per JSON
        for e in entries:
            e["_chart_category"] = chart["category"]

        all_entries.extend(entries)

        # Push a Supabase se configurato
        if args.supabase_url and args.supabase_key and not args.json_only:
            try:
                # Pulisci entries per RPC (rimuovi campi interni)
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
                    print(f"   → DB: ok (response: {result})")
            except Exception as e:
                print(f"   ⚠ Errore Supabase: {e}")
                print(f"   → Continuo con le altre classifiche...")

    # Salva sempre il JSON (retrocompatibilità con app HTML)
    save_json(all_entries, args.output, chart_date)

    print(f"\n{'='*50}")
    print(f"Totale: {len(all_entries)} titoli da {len(CHARTS)} classifiche")
    if total_ingested > 0:
        print(f"Supabase: {total_ingested} ingested, {total_matched} matched con inventario")
    print("Done!")


if __name__ == "__main__":
    main()
