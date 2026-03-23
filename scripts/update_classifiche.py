#!/usr/bin/env python3
"""
Scraper classifiche IBS → Supabase ranking_charts
Versione 3: URL IBS aggiornati marzo 2026 + scraping robusto.

Uso:
  python scripts/update_classifiche.py --supabase-url URL --supabase-key KEY
  python scripts/update_classifiche.py --output classifiche.json  # solo JSON
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ── URL IBS aggiornati (formato marzo 2026) ─────────────────────
CHARTS = [
    {
        "name": "Top libri",
        "category": "generale",
        "url": "https://www.ibs.it/classifica/libri/1week/sold"
    },
    {
        "name": "Narrativa italiana",
        "category": "narrativa_italiana",
        "url": "https://www.ibs.it/classifica/libri_narrativa-italiana/1week/sold"
    },
    {
        "name": "Narrativa straniera",
        "category": "narrativa_straniera",
        "url": "https://www.ibs.it/classifica/libri_narrativa-straniera/1week/sold"
    },
    {
        "name": "Bambini e ragazzi",
        "category": "bambini_ragazzi",
        "url": "https://www.ibs.it/classifica/libri_bambini-ragazzi/1week/sold"
    },
    {
        "name": "Religione e spiritualità",
        "category": "religione",
        "url": "https://www.ibs.it/classifica/libri_religione-spiritualita/1week/sold"
    },
    {
        "name": "Fumetti e graphic novel",
        "category": "fumetti",
        "url": "https://www.ibs.it/classifica/libri_fumetti-graphic-novels/1week/sold"
    },
    {
        "name": "Gialli e thriller",
        "category": "gialli_thriller",
        "url": "https://www.ibs.it/classifica/libri_gialli-thriller-horror/1week/sold"
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}


def extract_ean_from_url(url: str) -> str | None:
    """Prova a estrarre un EAN/ISBN dalla URL del prodotto IBS."""
    m = re.search(r'/e/(\d{13})$', url or '')
    if m:
        ean = m.group(1)
        if ean.startswith(('978', '979')):
            return ean
    return None


def scrape_chart(chart: dict, max_pages: int = 3) -> list[dict]:
    """Scrapa una classifica IBS e restituisce lista di entries."""
    entries = []
    position = 1

    for page in range(1, max_pages + 1):
        url = chart["url"] if page == 1 else f"{chart['url']}?defaultPage={page}"
        print(f"  Fetching {url} ...")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 404:
                print(f"  ⚠ 404 - URL non valido, skip")
                break
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"  ⚠ HTTP error pagina {page}: {e}")
            break
        except Exception as e:
            print(f"  ⚠ Errore fetch pagina {page}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # IBS 2026: i prodotti sono in blocchi con link al prodotto e info
        # Proviamo diversi selettori
        items = soup.select("div.cc-showcase-product")
        if not items:
            items = soup.select("div[data-product-id]")
        if not items:
            items = soup.select("li.cc-showcase-product")
        if not items:
            # Fallback: cerca tutti i link prodotto con /e/ pattern
            product_links = soup.select('a[href*="/e/"]')
            # Raggruppa per prodotto unico
            seen_urls = set()
            for link in product_links:
                href = link.get('href', '')
                if '/e/' in href and href not in seen_urls:
                    seen_urls.add(href)
                    title_text = link.get_text(strip=True)
                    if title_text and len(title_text) > 3:
                        entry = {"position": position}
                        ean = extract_ean_from_url(href)
                        if ean:
                            entry["ean"] = ean
                        entry["title"] = title_text[:200]
                        entries.append(entry)
                        position += 1
            if entries:
                print(f"   Trovati {len(entries)} titoli (via link)")
            else:
                print(f"  ⚠ Nessun prodotto trovato a pagina {page}")
            if page == 1 and not entries:
                break
            continue

        for item in items:
            entry = {"position": position}

            # EAN da data attribute o link
            ean = item.get("data-product-ean", "")
            if not ean:
                link = item.select_one('a[href*="/e/"]')
                if link:
                    ean = extract_ean_from_url(link.get('href', ''))
            if ean and re.match(r'^97[89]\d{10}$', str(ean).strip()):
                entry["ean"] = str(ean).strip()

            # Titolo
            title_el = item.select_one(
                "h2, h3, .cc-product-title, .product-title, "
                "[class*='title'] a, [class*='product-name']"
            )
            if title_el:
                entry["title"] = title_el.get_text(strip=True)[:200]
            else:
                entry["title"] = f"Posizione {position}"

            # Autore
            author_el = item.select_one(
                ".cc-product-author, .product-author, [class*='author']"
            )
            if author_el:
                author_text = author_el.get_text(strip=True)
                # Rimuovi prefisso "di " comune su IBS
                if author_text.lower().startswith("di "):
                    author_text = author_text[3:]
                entry["author"] = author_text[:100]

            # Editore
            pub_el = item.select_one(
                ".cc-product-publisher, .product-publisher, [class*='publisher']"
            )
            if pub_el:
                entry["publisher"] = pub_el.get_text(strip=True)[:100]

            entries.append(entry)
            position += 1

        time.sleep(1.5)

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


def save_json(all_entries: list[dict], output_path: str):
    """Salva il feed JSON per retrocompatibilità."""
    feed = {
        "source": "Scraper classifiche IBS v3",
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
    parser = argparse.ArgumentParser(description="Scraper classifiche IBS v3")
    parser.add_argument("--supabase-url", help="URL progetto Supabase")
    parser.add_argument("--supabase-key", help="Supabase service key")
    parser.add_argument("--output", default="classifiche.json")
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_entries = []
    total_ingested = 0
    total_matched = 0

    print(f"=== Scraper classifiche IBS v3 — {chart_date} ===\n")

    for chart in CHARTS:
        print(f"\n📊 {chart['name']} ({chart['category']})")
        entries = scrape_chart(chart, max_pages=args.max_pages)
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
                else:
                    print(f"   → DB: ok")
            except Exception as e:
                print(f"   ⚠ Errore Supabase: {e}")

    save_json(all_entries, args.output)

    print(f"\n{'='*50}")
    print(f"Totale: {len(all_entries)} titoli da {len(CHARTS)} classifiche")
    if total_ingested > 0:
        print(f"Supabase: {total_ingested} ingested, {total_matched} matched")
    print("Done!")


if __name__ == "__main__":
    main()
