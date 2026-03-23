#!/usr/bin/env python3
"""
Scraper classifiche IBS → Supabase ranking_charts
Versione 5: Playwright con estrazione JS robusta.
"""

import argparse
import json
import re
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

# JavaScript da iniettare nella pagina per estrarre i prodotti
EXTRACT_JS = """
() => {
    const results = [];
    // IBS usa diverse strutture. Proviamo tutte.
    
    // Strategia 1: cerca i div della classifica con data-product-id o data-ean
    let items = document.querySelectorAll('[data-product-id], [data-ean]:not([data-site-position="menu"])');
    
    // Strategia 2: cerca nella zona main/content, escludendo menu/header/footer
    if (items.length === 0) {
        const mainArea = document.querySelector('main, #main, .main-content, .cc-content, [role="main"], .page-main');
        if (mainArea) {
            items = mainArea.querySelectorAll('a[href*="/e/"]');
        }
    }
    
    // Strategia 3: cerca cc-showcase-product o simili
    if (items.length === 0) {
        items = document.querySelectorAll('.cc-showcase-product, .cc-product-item, .product-item-info');
    }
    
    // Strategia 4: intercetta i dati dal dataLayer di Google Analytics
    if (items.length === 0 && window.dataLayer) {
        for (const entry of window.dataLayer) {
            if (entry.ecommerce && entry.ecommerce.impressions) {
                for (const imp of entry.ecommerce.impressions) {
                    results.push({
                        title: imp.name || '',
                        ean: imp.id || '',
                        author: imp.brand || '',
                        position: imp.position || results.length + 1,
                        category: imp.category || ''
                    });
                }
                return results;
            }
        }
    }
    
    // Strategia 5: cerca staticImpressions (IBS li usa per analytics)
    if (items.length === 0 && window.staticImpressions) {
        for (const key of Object.keys(window.staticImpressions)) {
            const impressions = window.staticImpressions[key];
            if (Array.isArray(impressions)) {
                for (const imp of impressions) {
                    results.push({
                        title: (imp.item_name || '').replace(/_/g, ' '),
                        ean: imp.item_id || '',
                        position: (imp.index || results.length) + 1,
                        category: imp.item_category2 || ''
                    });
                }
                return results;
            }
        }
    }
    
    // Processa gli items trovati con strategie 1-3
    const seen = new Set();
    let pos = 1;
    items.forEach(item => {
        // Trova il link al prodotto
        const link = item.tagName === 'A' ? item : item.querySelector('a[href*="/e/"]');
        if (!link) return;
        
        const href = link.getAttribute('href') || '';
        if (seen.has(href) || !href.includes('/e/')) return;
        
        // Escludi link del menu
        if (link.getAttribute('data-site-position') === 'menu') return;
        if (link.closest('nav, header, .menu, .cc-header')) return;
        
        seen.add(href);
        
        // Estrai EAN dalla URL
        const eanMatch = href.match(/\\/e\\/(\\d{13})$/);
        const ean = eanMatch ? eanMatch[1] : '';
        
        // Titolo
        const titleEl = item.querySelector('h2, h3, .cc-product-title, [class*="title"]') || link;
        const title = (titleEl.textContent || '').trim();
        
        if (!title || title.length < 3) return;
        
        // Autore
        const authorEl = item.querySelector('[class*="author"]');
        let author = authorEl ? authorEl.textContent.trim() : '';
        if (author.toLowerCase().startsWith('di ')) author = author.substring(3);
        
        results.push({
            title: title.substring(0, 200),
            ean: ean,
            author: author.substring(0, 100),
            position: pos++
        });
    });
    
    return results;
}
"""


def scrape_chart_playwright(page, chart: dict) -> list[dict]:
    url = chart["url"]
    print(f"  Navigating to {url} ...")

    try:
        page.goto(url, wait_until="networkidle", timeout=45000)
        # Aspetta che la pagina finisca di caricare i dati
        time.sleep(5)
    except Exception as e:
        print(f"  ⚠ Errore navigazione: {e}")
        # Prova comunque a estrarre
        time.sleep(3)

    try:
        entries = page.evaluate(EXTRACT_JS)
        if entries:
            # Pulisci EAN
            for e in entries:
                ean = str(e.get("ean", ""))
                if not re.match(r'^97[89]\d{10}$', ean):
                    e.pop("ean", None)
            return entries
    except Exception as e:
        print(f"  ⚠ Errore estrazione JS: {e}")

    # Fallback: prova a prendere il contenuto HTML e parsarlo
    try:
        content = page.content()
        # Cerca staticImpressions nel source
        match = re.search(r'staticImpressions\[.*?\]\s*=\s*(\[.*?\]);', content, re.DOTALL)
        if match:
            data = json.loads(match.group(1))
            entries = []
            for i, item in enumerate(data):
                entry = {
                    "position": i + 1,
                    "title": (item.get("item_name") or "").replace("_", " "),
                }
                ean = item.get("item_id", "")
                if re.match(r'^97[89]\d{10}$', str(ean)):
                    entry["ean"] = str(ean)
                if entry["title"]:
                    entries.append(entry)
            if entries:
                print(f"  → Trovati via staticImpressions nel source")
                return entries
    except Exception as e:
        print(f"  ⚠ Errore fallback: {e}")

    return []


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
        "source": "Scraper classifiche IBS v5",
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

    from playwright.sync_api import sync_playwright

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_entries = []
    total_ingested = 0
    total_matched = 0

    print(f"=== Scraper classifiche IBS v5 — {chart_date} ===\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
            locale="it-IT",
            viewport={"width": 1280, "height": 900},
        )
        # Blocca cookie banner per non interferire
        context.add_cookies([{
            "name": "CookieConsent",
            "value": "{stamp:%27-1%27%2Cnecessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false}",
            "domain": ".ibs.it",
            "path": "/",
        }])
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
