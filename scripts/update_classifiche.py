#!/usr/bin/env python3
"""
RADAR COMPLETO: Classifiche + Eventi Locali + Manga Trend → Supabase
Versione 9.1: Fix duplicati Giunti + Scraper robusti anti-rottura
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

def extract_isbn(url: str) -> str | None:
    m = re.search(r'(97[89]\d{10})', url or '')
    return m.group(1) if m else None

def fetch_page(url: str) -> str | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  ⚠ Errore fetch: {e}")
        return None

# ═══════════════════════════════════════════════════════
# CLASSIFICHE
# ═══════════════════════════════════════════════════════

def parse_libraccio(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries, seen, pos = [], set(), 1
    for link in soup.select('a[href*="/libro/"]'):
        href = link.get("href", "")
        if href in seen or "/autore/" in href: continue
        title = link.get_text(strip=True)
        if not title or len(title) < 4: continue
        seen.add(href)
        entry = {"position": pos, "title": title[:200]}
        ean = extract_isbn(href)
        if ean: entry["ean"] = ean
        parent = link.find_parent(["div", "li", "td"])
        if parent:
            auth = parent.select_one('a[href*="/autore/"]')
            if auth: entry["author"] = auth.get_text(strip=True)[:100]
        entries.append(entry)
        pos += 1
    return entries

def parse_giunti(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries, seen_keys, pos = [], set(), 1
    for link in soup.select('a[href*="/products/"]'):
        href = link.get("href", "")
        title = link.get_text(strip=True)
        if not title or len(title) < 4: continue
        if title.lower() in ("vedi tutto", "scopri", "aggiungi", "acquista"): continue
        
        ean = extract_isbn(href)
        
        # FIX DUPLICATI: Creiamo una chiave unica usando l'EAN o il Titolo
        chiave_dedup = ean if ean else title.lower()
        if chiave_dedup in seen_keys: 
            continue # Lo abbiamo già visto, saltalo!
        seen_keys.add(chiave_dedup)
        
        entry = {"position": pos, "title": title[:200]}
        if ean: entry["ean"] = ean
        parent = link.find_parent(["div", "li", "article"])
        if parent:
            for el in parent.find_all(string=True):
                text = el.strip()
                if text and text != title and 3 < len(text) < 80 and not text.startswith("€"):
                    entry["author"] = text[:100]
                    break
        entries.append(entry)
        pos += 1
    return entries

RANKING_CHARTS = [
    {"name": "Libraccio Top 100", "category": "generale", "source_key": "libraccio", "url": "https://www.libraccio.it/Top100.aspx", "parser": parse_libraccio},
    {"name": "Libraccio Narrativa", "category": "narrativa", "source_key": "libraccio", "url": "https://www.libraccio.it/reparto/32/narrativa.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Ragazzi", "category": "bambini_ragazzi", "source_key": "libraccio", "url": "https://www.libraccio.it/reparto/27/libri-per-ragazzi.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Fumetti", "category": "fumetti", "source_key": "libraccio", "url": "https://www.libraccio.it/reparto/21/fumetti-e-graphic-novels.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Libraccio Religione", "category": "religione", "source_key": "libraccio", "url": "https://www.libraccio.it/reparto/35/religione.html?ordinamento=piu-venduti", "parser": parse_libraccio},
    {"name": "Giunti Top 20", "category": "generale", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/classifica-gap", "parser": parse_giunti},
    {"name": "Giunti Gialli Thriller", "category": "gialli_thriller", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/gialli-e-thriller", "parser": parse_giunti},
    {"name": "Giunti Bambini Ragazzi", "category": "bambini_ragazzi", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/bambini-e-ragazzi", "parser": parse_giunti},
    {"name": "Giunti TikTok", "category": "social_trend", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/i-piu-amati-su-tik-tok", "parser": parse_giunti},
    {"name": "Giunti Fantasy", "category": "fantasy", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/fantasy", "parser": parse_giunti},
    {"name": "Giunti Young Adult", "category": "young_adult", "source_key": "giunti", "url": "https://giuntialpunto.it/collections/young-adult", "parser": parse_giunti},
]

# ═══════════════════════════════════════════════════════
# EVENTI LOCALI LUNIGIANA
# ═══════════════════════════════════════════════════════

def parse_sigeric_events(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    events = []
    articles = soup.select("article, .event-item, .post, .entry, .wp-block-post")
    if not articles: articles = soup.select('a[href*="evento"], a[href*="event"], a[href*="/tour/"]')
    for art in articles:
        title_el = art.select_one("h2, h3, h4, .entry-title, .event-title") or art
        title = title_el.get_text(strip=True)[:200]
        if not title or len(title) < 5: continue
        link_el = art.select_one("a[href]") if art.name != 'a' else art
        url = link_el.get("href", "") if link_el else ""
        text = art.get_text(" ", strip=True)
        date_match = re.search(r'(\d{1,2})\s*(?:e\s*\d{1,2}\s*)?(?:gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s*(20\d{2})', text, re.IGNORECASE)
        event_date = None
        if date_match:
            months = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04","maggio":"05","giugno":"06","luglio":"07","agosto":"08","settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}
            for m_name, m_num in months.items():
                if m_name in text.lower():
                    day = date_match.group(1).zfill(2)
                    year = date_match.group(2)
                    event_date = f"{year}-{m_num}-{day}"
                    break
        events.append({"title": title, "url": url, "event_date": event_date, "source_text": text[:500]})
    return events

def parse_visitlunigiana_events(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    events = []
    articles = soup.select(".tribe_events, .type-tribe_events, article, .event-item")
    if not articles: articles = soup.select('a[href*="/events/"], a[href*="/evento/"]')
    for art in articles:
        title_el = art.select_one("h2, h3, .tribe-events-list-event-title, .entry-title") or art
        title = title_el.get_text(strip=True)[:200]
        if not title or len(title) < 5: continue
        link_el = art.select_one("a[href]") if art.name != 'a' else art
        url = link_el.get("href", "") if link_el else ""
        date_el = art.select_one(".tribe-event-schedule-details, time, .event-date, [datetime]")
        event_date = None
        if date_el:
            dt = date_el.get("datetime", "")
            if dt: event_date = dt[:10]
        events.append({"title": title, "url": url, "event_date": event_date})
    return events

def parse_lunigianaworld_events(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    events, seen = [], set()
    for link in soup.find_all('a', href=True):
        href = link.get("href", "")
        if ('evento' in href.lower() or 'eventi' in href.lower() or 'calendario' in href.lower() or 'tour' in href.lower()) and href not in seen:
            title = link.get_text(strip=True)[:200]
            if len(title) > 5 and title.lower() not in ('leggi tutto', 'read more', 'calendario eventi', 'scopri'):
                seen.add(href)
                events.append({"title": title, "url": href})
    return events

def parse_sagretoscane(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    events, seen = [], set()
    for link in soup.find_all('a', href=True):
        href = link.get("href", "")
        if ('/eventi/' in href or 'sagra' in href.lower() or 'festa' in href.lower()) and href not in seen:
            title = link.get_text(strip=True)[:200]
            if len(title) > 5 and title.lower() not in ('leggi tutto', 'continua', 'scopri di più'):
                seen.add(href)
                parent = link.find_parent(["div", "li", "article", "td"])
                event_date = None
                if parent:
                    text = parent.get_text(" ", strip=True)
                    date_match = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](20\d{2})', text)
                    if date_match:
                        event_date = f"{date_match.group(3)}-{date_match.group(2).zfill(2)}-{date_match.group(1).zfill(2)}"
                
                if href.startswith('/'):
                    href = f"https://www.sagretoscane.com{href}"
                    
                events.append({"title": title, "url": href, "event_date": event_date})
    return events

LOCAL_EVENT_SOURCES = [
    {"name": "Sigeric Eventi", "source_key": "sigeric", "url": "https://www.sigeric.it/", "parser": parse_sigeric_events},
    {"name": "VisitLunigiana Eventi", "source_key": "visitlunigiana", "url": "https://visitlunigiana.it/events/", "parser": parse_visitlunigiana_events},
    {"name": "VisitLunigiana Lista", "source_key": "visitlunigiana", "url": "https://visitlunigiana.it/eventi-in-lunigiana/", "parser": parse_visitlunigiana_events},
    {"name": "Lunigiana World", "source_key": "lunigianaworld", "url": "https://www.lunigianaworld.it/calendario-eventi/", "parser": parse_lunigianaworld_events},
    {"name": "Sagre Toscane Lunigiana", "source_key": "sagretoscane", "url": "https://www.sagretoscane.com/eventi/lunigiana/", "parser": parse_sagretoscane},
]

# ═══════════════════════════════════════════════════════
# MANGA / ANIME TREND
# ═══════════════════════════════════════════════════════

def parse_animeclick_news(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries, seen = [], set()
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/news/" in href and re.search(r'\d+-', href) and href not in seen and "#comments" not in href:
            title = link.get_text(strip=True)[:200]
            if len(title) > 10:
                seen.add(href)
                full_url = href if href.startswith("http") else f"https://www.animeclick.it{href}"
                entries.append({"title": title, "url": full_url})
    return entries[:30]

MANGA_SOURCES = [
    {"name": "Libraccio Fumetti", "category": "fumetti", "source_key": "libraccio", "url": "https://www.libraccio.it/reparto/21/fumetti-e-graphic-novels.html?ordinamento=piu-venduti", "parser": parse_libraccio, "type": "ranking"},
    {"name": "AnimeClick News", "source_key": "animeclick", "url": "https://www.animeclick.it/news", "parser": parse_animeclick_news, "type": "trend"},
]

# ═══════════════════════════════════════════════════════
# SUPABASE INTEGRATION
# ═══════════════════════════════════════════════════════

def push_rankings(supabase_url, supabase_key, chart, entries, chart_date):
    rpc_url = f"{supabase_url}/rest/v1/rpc/ingest_ranking_chart"
    payload = {"p_source_key": chart["source_key"], "p_chart_name": chart["name"], "p_chart_category": chart["category"], "p_period": "weekly", "p_chart_date": chart_date, "p_entries": entries}
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}", "Content-Type": "application/json", "Prefer": "return=representation"}
    resp = requests.post(rpc_url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def push_events(supabase_url, supabase_key, source_key, events):
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}", "Content-Type": "application/json", "Prefer": "return=minimal"}
    inserted = 0
    for ev in events:
        row = {"feed_key": f"events-{source_key}", "source_key": source_key, "raw_title": ev.get("title", ""), "signal_type": "local_event", "signal_text": ev.get("title", ""), "event_date": ev.get("event_date"), "source_url": ev.get("url", ""), "source_payload": json.dumps(ev), "processed": False}
        try:
            resp = requests.post(f"{supabase_url}/rest/v1/external_signal_staging", json=row, headers=headers, timeout=10)
            if resp.status_code in (200, 201): inserted += 1
        except: pass
    return inserted

def push_manga_trends(supabase_url, supabase_key, source_key, trends):
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}", "Content-Type": "application/json", "Prefer": "return=minimal"}
    inserted = 0
    for t in trends:
        row = {"feed_key": f"manga-{source_key}", "source_key": source_key, "raw_title": t.get("title", ""), "signal_type": "manga_trend", "signal_text": t.get("title", ""), "source_url": t.get("url", ""), "source_payload": json.dumps(t), "processed": False}
        try:
            resp = requests.post(f"{supabase_url}/rest/v1/external_signal_staging", json=row, headers=headers, timeout=10)
            if resp.status_code in (200, 201): inserted += 1
        except: pass
    return inserted

def save_json(all_rankings, all_events, all_manga, output_path):
    feed = {
        "source": "Radar completo v9.1",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rankings": [{"ean": e.get("ean", ""), "title": e.get("title", ""), "author": e.get("author", ""), "position": e.get("position", 0), "category": e.get("_category", ""), "source": e.get("_source", "")} for e in all_rankings],
        "local_events": [{"title": e.get("title", ""), "date": e.get("event_date", ""), "url": e.get("url", ""), "source": e.get("_source", "")} for e in all_events],
        "manga_trends": [{"title": e.get("title", ""), "source": e.get("_source", ""), "url": e.get("url", "")} for e in all_manga],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    total = len(feed["rankings"]) + len(feed["local_events"]) + len(feed["manga_trends"])
    print(f"\n✓ JSON salvato: {output_path} ({total} segnali totali)")

# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supabase-url")
    parser.add_argument("--supabase-key")
    parser.add_argument("--output", default="classifiche.json")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_rankings, all_events, all_manga = [], [], []
    has_db = args.supabase_url and args.supabase_key and not args.json_only

    print(f"{'='*60}\n  RADAR COMPLETO v9.1 — {chart_date}\n{'='*60}\n")

    print("━━━ CLASSIFICHE ━━━")
    for chart in RANKING_CHARTS:
        print(f"\n📊 [{chart['source_key'].upper()}] {chart['name']}")
        html = fetch_page(chart["url"])
        if not html: continue
        entries = chart["parser"](html)
        print(f"   Trovati {len(entries)} titoli")
        if not entries: continue
        for e in entries:
            e["_category"] = chart["category"]
            e["_source"] = chart["source_key"]
        all_rankings.extend(entries)
        if has_db:
            try:
                clean = [{k: v for k, v in e.items() if not k.startswith("_")} for e in entries]
                result = push_rankings(args.supabase_url, args.supabase_key, chart, clean, chart_date)
                if isinstance(result, list) and len(result) > 0:
                    r = result[0]
                    print(f"   → DB: {r.get('ingested',0)} ingested, {r.get('matched',0)} matched")
            except Exception as e:
                print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    print("\n\n━━━ EVENTI LOCALI LUNIGIANA ━━━")
    for src in LOCAL_EVENT_SOURCES:
        print(f"\n📍 [{src['source_key'].upper()}] {src['name']}")
        html = fetch_page(src["url"])
        if not html: continue
        events = src["parser"](html)
        print(f"   Trovati {len(events)} eventi")
        if not events: continue
        for e in events: e["_source"] = src["source_key"]
        all_events.extend(events)
        if has_db:
            try:
                n = push_events(args.supabase_url, args.supabase_key, src["source_key"], events)
                print(f"   → DB: {n} eventi inseriti in staging")
            except Exception as e:
                print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    print("\n\n━━━ MANGA / ANIME TREND ━━━")
    for src in MANGA_SOURCES:
        print(f"\n🔥 [{src['source_key'].upper()}] {src['name']}")
        html = fetch_page(src["url"])
        if not html: continue
        
        if src.get("type") == "ranking":
            entries = src["parser"](html)
            print(f"   Trovati {len(entries)} titoli")
        else:
            trends = src["parser"](html)
            print(f"   Trovati {len(trends)} trend/news")
            for t in trends: t["_source"] = src["source_key"]
            all_manga.extend(trends)
            if has_db:
                try:
                    n = push_manga_trends(args.supabase_url, args.supabase_key, src["source_key"], trends)
                    print(f"   → DB: {n} trend inseriti in staging")
                except Exception as e:
                    print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    save_json(all_rankings, all_events, all_manga, args.output)
    print(f"\n  Done!")

if __name__ == "__main__":
    main()
