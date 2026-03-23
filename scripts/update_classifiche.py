#!/usr/bin/env python3
"""
Scraper classifiche IBS → Supabase ranking_charts
Versione 6: aggiunge debug screenshot + HTML dump per diagnostica.
"""

import argparse
import json
import os
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

EXTRACT_JS = """
() => {
    const results = [];
    const debug = {strategies_tried: [], page_title: document.title, url: location.href};
    
    // Strategia 1: staticImpressions (IBS analytics object)
    debug.strategies_tried.push('staticImpressions');
    if (window.staticImpressions) {
        debug.staticImpressions_keys = Object.keys(window.staticImpressions);
        for (const key of Object.keys(window.staticImpressions)) {
            const impressions = window.staticImpressions[key];
            if (Array.isArray(impressions) && impressions.length > 0) {
                debug.staticImpressions_found = impressions.length;
                for (const imp of impressions) {
                    results.push({
                        title: (imp.item_name || '').replace(/_/g, ' '),
                        ean: imp.item_id || '',
                        position: (imp.index != null ? imp.index : results.length) + 1,
                        publisher: imp.item_brand || '',
                        category: imp.item_category2 || ''
                    });
                }
                return {results, debug};
            }
        }
    }
    
    // Strategia 2: dataLayer
    debug.strategies_tried.push('dataLayer');
    if (window.dataLayer) {
        debug.dataLayer_length = window.dataLayer.length;
        for (const entry of window.dataLayer) {
            if (entry.ecommerce) {
                const impressions = entry.ecommerce.impressions || 
                                   (entry.ecommerce.promoView && entry.ecommerce.promoView.promotions) || [];
                if (impressions.length > 0) {
                    debug.dataLayer_impressions = impressions.length;
                    for (const imp of impressions) {
                        results.push({
                            title: imp.name || '',
                            ean: imp.id || '',
                            author: imp.brand || '',
                            position: imp.position || results.length + 1,
                        });
                    }
                    return {results, debug};
                }
            }
        }
    }
    
    // Strategia 3: cerca nel DOM i prodotti, escludendo menu
    debug.strategies_tried.push('DOM_main_area');
    const mainSelectors = [
        'main', '#maincontent', '.page-main', '.cc-content-area',
        '[role="main"]', '.cc-page-content', '#content'
    ];
    let mainArea = null;
    for (const sel of mainSelectors) {
        mainArea = document.querySelector(sel);
        if (mainArea) { debug.main_selector = sel; break; }
    }
    
    if (mainArea) {
        const allLinks = mainArea.querySelectorAll('a[href*="/e/"]');
        debug.main_links_count = allLinks.length;
        const seen = new Set();
        let pos = 1;
        allLinks.forEach(link => {
            const href = link.getAttribute('href') || '';
            if (seen.has(href)) return;
            if (link.closest('nav, header, .cc-header, [data-site-position="menu"]')) return;
            seen.add(href);
            const eanMatch = href.match(/\\/e\\/(\\d{13})$/);
            const title = (link.textContent || '').trim();
            if (title && title.length > 3 && title.length < 300) {
                results.push({
                    title: title.substring(0, 200),
                    ean: eanMatch ? eanMatch[1] : '',
                    position: pos++
                });
            }
        });
    }
    
    // Strategia 4: cerca qualsiasi dato in window che sembri una lista prodotti
    if (results.length === 0) {
        debug.strategies_tried.push('window_scan');
        const interesting = [];
        for (const key of Object.keys(window)) {
            try {
                const val = window[key];
                if (Array.isArray(val) && val.length > 5 && val.length < 500) {
                    if (val[0] && (val[0].item_name || val[0].name || val[0].title || val[0].ean)) {
                        interesting.push(key);
                    }
                }
            } catch(e) {}
        }
        debug.interesting_window_keys = interesting;
    }
    
    debug.total_results = results.length;
    // Dump some page structure info
    debug.body_children = Array.from(document.body.children).map(el => 
        el.tagName + (el.id ? '#'+el.id : '') + (el.className ? '.'+el.className.split(' ')[0] : '')
    ).slice(0, 20);
    
    return {results, debug};
}
"""


def scrape_chart_playwright(page, chart, save_debug=False):
    url = chart["url"]
    print(f"  Navigating to {url} ...")

    try:
        page.goto(url, wait_until="networkidle", timeout=45000)
        time.sleep(5)
    except Exception as e:
        print(f"  ⚠ Errore navigazione: {e}")
        time.sleep(3)

    # Debug: save screenshot for first chart
    if save_debug:
        try:
            page.screenshot(path="debug_screenshot.png", full_page=True)
            print(f"  📸 Screenshot salvato: debug_screenshot.png")
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print(f"  📄 HTML salvato: debug_page.html")
        except Exception as e:
            print(f"  ⚠ Debug save error: {e}")

    try:
        result = page.evaluate(EXTRACT_JS)
        debug = result.get("debug", {})
        entries = result.get("results", [])
        
        # Print debug info
        print(f"  🔍 Debug: strategies={debug.get('strategies_tried')}")
        print(f"  🔍 Debug: title='{debug.get('page_title','?')}'")
        if 'staticImpressions_keys' in debug:
            print(f"  🔍 Debug: staticImpressions keys={debug['staticImpressions_keys']}")
        if 'staticImpressions_found' in debug:
            print(f"  🔍 Debug: staticImpressions found={debug['staticImpressions_found']}")
        if 'dataLayer_length' in debug:
            print(f"  🔍 Debug: dataLayer entries={debug['dataLayer_length']}")
        if 'dataLayer_impressions' in debug:
            print(f"  🔍 Debug: dataLayer impressions={debug['dataLayer_impressions']}")
        if 'main_selector' in debug:
            print(f"  🔍 Debug: main area='{debug['main_selector']}', links={debug.get('main_links_count',0)}")
        if 'interesting_window_keys' in debug:
            print(f"  🔍 Debug: interesting window keys={debug['interesting_window_keys']}")
        if 'body_children' in debug:
            print(f"  🔍 Debug: body structure={debug['body_children'][:10]}")
        
        # Clean EAN
        for e in entries:
            ean = str(e.get("ean", ""))
            if not re.match(r'^97[89]\d{10}$', ean):
                e.pop("ean", None)
        
        return entries
    except Exception as e:
        print(f"  ⚠ Errore estrazione: {e}")
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
        "source": "Scraper classifiche IBS v6",
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

    print(f"=== Scraper classifiche IBS v6 (debug) — {chart_date} ===\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
            locale="it-IT",
            viewport={"width": 1280, "height": 900},
        )
        context.add_cookies([{
            "name": "CookieConsent",
            "value": "{stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true}",
            "domain": ".ibs.it",
            "path": "/",
        }])
        page = context.new_page()

        for i, chart in enumerate(CHARTS):
            print(f"\n📊 {chart['name']} ({chart['category']})")
            entries = scrape_chart_playwright(page, chart, save_debug=(i == 0))
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
