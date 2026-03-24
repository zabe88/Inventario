#!/usr/bin/env python3
"""
RADAR COMPLETO: Classifiche + Eventi Locali + Manga Trend + YouTube → Supabase
Versione 11.0: Integrazione API YouTube Data v3 per BookTok/Manga.
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

VISITLUNIGIANA_HEADERS = {
    **HEADERS,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.5",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def extract_isbn(url: str) -> str | None:
    m = re.search(r'(97[89]\d{10})', url or '')
    return m.group(1) if m else None


def fetch_page(url: str) -> str | None:
    headers = VISITLUNIGIANA_HEADERS if "visitlunigiana.it" in (url or "") else HEADERS
    last_error = None
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 403 and "visitlunigiana.it" in (url or ""):
                # 403 frequente su runner cloud: ritenta con lieve backoff, mantenendo headers più realistici
                time.sleep(1.5 * (attempt + 1))
                last_error = RuntimeError(f"403 Forbidden su {url}")
                continue
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_error = e
            time.sleep(1.2 * (attempt + 1))
    print(f"  ⚠ Errore fetch: {last_error}")
    return None


# ═══════════════════════════════════════════════════════
# YOUTUBE BOOKTOK / MANGA TREND
# ═══════════════════════════════════════════════════════

def fetch_youtube_trends(api_key: str) -> list[dict]:
    """Cerca video in trend su YouTube (ultimi 7 gg) per BookTok e Manga"""
    if not api_key:
        print("   ⚠ YouTube API Key mancante. Salto YouTube.")
        return []

    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        
        # Cerchiamo video degli ultimi 7 giorni
        week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat() + 'Z'
        
        queries = ["booktok ita", "manga consigliati", "recensione fumetto", "nuovi manga"]
        entries = []
        seen_videos = set()

        for q in queries:
            print(f"   Ricerca YT: '{q}'...")
            request = youtube.search().list(
                part="snippet",
                q=q,
                type="video",
                publishedAfter=week_ago,
                regionCode="IT",  # Solo Italia
                maxResults=10,
                order="viewCount" # Ordina per i più visti
            )
            response = request.execute()

            for item in response.get("items", []):
                vid = item["id"]["videoId"]
                if vid in seen_videos: continue
                seen_videos.add(vid)
                
                title = item["snippet"]["title"]
                channel = item["snippet"]["channelTitle"]
                
                entries.append({
                    "title": f"[{channel}] {title}",
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "source_text": item["snippet"]["description"][:500]
                })
            time.sleep(1) # Rispettiamo i rate limit

        return entries

    except Exception as e:
        print(f"   ⚠ Errore YouTube API: {e}")
        return []

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
    entries, seen_titles, pos = [], set(), 1
    for link in soup.select('a[href*="/products/"]'):
        title = link.get_text(strip=True)
        if not title or len(title) < 4: continue
        if title.lower() in ("vedi tutto", "scopri", "aggiungi"): continue
        
        # Deduplicazione basata sul titolo normalizzato, non sull'URL
        norm_title = re.sub(r'[^a-z0-9]', '', title.lower())
        if norm_title in seen_titles: continue
        seen_titles.add(norm_title)
        
        entry = {"position": pos, "title": title[:200]}
        href = link.get("href", "")
        ean = extract_isbn(href)
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
    {"name": "Giunti Top 20", "category": "generale", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/classifica-gap", "parser": parse_giunti},
    {"name": "Giunti Gialli Thriller", "category": "gialli_thriller", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/gialli-e-thriller", "parser": parse_giunti},
    {"name": "Giunti Bambini Ragazzi", "category": "bambini_ragazzi", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/bambini-e-ragazzi", "parser": parse_giunti},
    {"name": "Giunti TikTok", "category": "social_trend", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/i-piu-amati-su-tik-tok", "parser": parse_giunti},
    {"name": "Giunti Fantasy", "category": "fantasy", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/fantasy", "parser": parse_giunti},
    {"name": "Giunti Young Adult", "category": "young_adult", "source_key": "giunti",
     "url": "https://giuntialpunto.it/collections/young-adult", "parser": parse_giunti},
]


# ═══════════════════════════════════════════════════════
# EVENTI LOCALI LUNIGIANA
# ═══════════════════════════════════════════════════════

def parse_sigeric_events(html: str) -> list[dict]:
    """Parsa eventi da sigeric.it"""
    soup = BeautifulSoup(html, "html.parser")
    events = []
    # Sigeric usa WordPress con post/eventi
    articles = soup.select("article, .event-item, .post, .entry, .wp-block-post")
    if not articles:
        # Fallback: cerca tutti i link con "evento" o date
        articles = soup.select('a[href*="evento"], a[href*="event"], a[href*="/tour/"]')
    
    for art in articles:
        title_el = art.select_one("h2, h3, h4, .entry-title, .event-title") or art
        title = title_el.get_text(strip=True)[:200]
        if not title or len(title) < 5: continue
        
        link_el = art.select_one("a[href]") if art.name != 'a' else art
        url = link_el.get("href", "") if link_el else ""
        
        # Cerca date nel testo
        text = art.get_text(" ", strip=True)
        date_match = re.search(r'(\d{1,2})\s*(?:e\s*\d{1,2}\s*)?(?:gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s*(20\d{2})', text, re.IGNORECASE)
        event_date = None
        if date_match:
            months = {"gennaio":"01","febbraio":"02","marzo":"03","aprile":"04","maggio":"05","giugno":"06",
                      "luglio":"07","agosto":"08","settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"}
            for m_name, m_num in months.items():
                if m_name in text.lower():
                    day = date_match.group(1).zfill(2)
                    year = date_match.group(2)
                    event_date = f"{year}-{m_num}-{day}"
                    break
        
        events.append({
            "title": title,
            "url": url,
            "event_date": event_date,
            "source_text": text[:500],
        })
    return events


def parse_visitlunigiana_events(html: str) -> list[dict]:
    """Parsa eventi da visitlunigiana.it"""
    soup = BeautifulSoup(html, "html.parser")
    events = []
    # VisitLunigiana usa The Events Calendar (WordPress)
    articles = soup.select(".tribe_events, .type-tribe_events, article, .event-item")
    if not articles:
        articles = soup.select('a[href*="/events/"], a[href*="/evento/"]')
    
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
            if dt:
                event_date = dt[:10]
        
        events.append({
            "title": title,
            "url": url,
            "event_date": event_date,
        })
    return events


def parse_lunigianaworld_events(html: str) -> list[dict]:
    """Parsa eventi da lunigianaworld.it"""
    soup = BeautifulSoup(html, "html.parser")
    events = []
    
    # Usiamo un approccio euristico: cerchiamo i blocchi che contengono le date
    for container in soup.select('.elementor-widget-wrap, article, .event-container, div'):
        text = container.get_text(" ", strip=True)
        # Il loro layout attuale stampa: "dal GG/MM/AAAA al GG/MM/AAAA [Comune] LOCANDINA"
        if "dal " in text and "al " in text and ("(MS)" in text or "LOCANDINA" in text):
            title_el = container.select_one("h2, h3, h4")
            if not title_el: continue
            
            title = title_el.get_text(strip=True)[:200]
            if not title or len(title) < 5: continue
            
            a_tag = container.select_one("a[href]")
            url = a_tag.get("href", "") if a_tag else ""
            
            # Estrazione data sicura
            date_match = re.search(r'dal (\d{2}/\d{2}/\d{4})', text)
            event_date = None
            if date_match:
                d, m, y = date_match.group(1).split('/')
                event_date = f"{y}-{m}-{d}"
            
            events.append({"title": title, "url": url, "event_date": event_date})
            
    # Deduplicazione finale (Elementor spesso duplica i widget in DOM per mobile/desktop)
    unique_events = {e["title"]: e for e in events}.values()
    return list(unique_events)


def parse_sagretoscane(html: str) -> list[dict]:
    """Parsa eventi da sagretoscane.com"""
    soup = BeautifulSoup(html, "html.parser")
    events = []
    
    # Se la pagina dichiara che non ci sono eventi, usciamo subito per non scrapare il widget laterale
    if "Nessun evento in programma al momento" in html:
        return []
        
    # Evitiamo la sidebar cercando nel blocco principale
    main_content = soup.select_one('.main-content, #content, .elenco-eventi, .ev-list') or soup
    
    for art in main_content.select("article, .evento, .sagra-item"):
        title_el = art.select_one("h2, h3, .title") or art
        title = title_el.get_text(strip=True)[:200]
        if not title or len(title) < 5: continue
        
        # Filtro extra per sicurezza
        if "Sagre in provincia" in title or "Eventi in" in title: continue
        
        link_el = art.select_one("a[href]") if art.name != 'a' else art
        url = link_el.get("href", "") if link_el else ""
        
        text = art.get_text(" ", strip=True)
        date_match = re.search(r'(\d{1,2})/(\d{1,2})/(20\d{2})', text)
        event_date = None
        if date_match:
            event_date = f"{date_match.group(3)}-{date_match.group(2).zfill(2)}-{date_match.group(1).zfill(2)}"
        
        events.append({"title": title, "url": url, "event_date": event_date})
    return events


LOCAL_EVENT_SOURCES = [
    {"name": "Sigeric Eventi", "source_key": "sigeric",
     "url": "https://www.sigeric.it/", "parser": parse_sigeric_events},
    {"name": "VisitLunigiana Eventi", "source_key": "visitlunigiana",
     "url": "https://visitlunigiana.it/events/", "parser": parse_visitlunigiana_events},
    {"name": "VisitLunigiana Lista", "source_key": "visitlunigiana",
     "url": "https://visitlunigiana.it/eventi-in-lunigiana/", "parser": parse_visitlunigiana_events},
    {"name": "Lunigiana World", "source_key": "lunigianaworld",
     "url": "https://www.lunigianaworld.it/calendario-eventi/", "parser": parse_lunigianaworld_events},
    {"name": "Sagre Toscane Lunigiana", "source_key": "sagretoscane",
     "url": "https://www.sagretoscane.com/eventi/lunigiana/", "parser": parse_sagretoscane},
]


# ═══════════════════════════════════════════════════════
# MANGA / ANIME TREND
# ═══════════════════════════════════════════════════════

def parse_mycomics_classifica(html: str) -> list[dict]:
    """Parsa classifica settimanale MyComics"""
    soup = BeautifulSoup(html, "html.parser")
    entries, seen, pos = [], set(), 1
    for link in soup.select('a[href*="/product/"], a[href*="/prodotto/"]'):
        href = link.get("href", "")
        if href in seen: continue
        title = link.get_text(strip=True)
        if not title or len(title) < 4: continue
        seen.add(href)
        entry = {"position": pos, "title": title[:200]}
        ean = extract_isbn(href)
        if ean: entry["ean"] = ean
        entries.append(entry)
        pos += 1
    return entries


def parse_animeclick_news(html: str) -> list[dict]:
    """Parsa news da AnimeClick per trend manga/anime"""
    soup = BeautifulSoup(html, "html.parser")
    entries = set()
    seen_urls = set()
    
    # Approccio a strascico: analizziamo tutti i link
    for link in soup.select('a[href]'):
        href = link.get("href", "")
        if not href.startswith('/') and not href.startswith('http'): continue
        if href in seen_urls: continue
        
        title = link.get_text(strip=True)[:200]
        # Scartiamo testi corti, roba di UI e menù
        if not title or len(title) < 15: continue
        if any(x in title for x in ["AnimeClick", "Accedi", "Registrati", "Commenti", "Leggi tutto"]): continue
        
        # Le vere news hanno titoli articolati (es. più di 4 parole)
        if len(title.split()) > 4: 
            seen_urls.add(href)
            full_url = href if href.startswith('http') else f"https://www.animeclick.it{href}"
            entries.add((title, full_url))
            
    return [{"title": t[0], "url": t[1]} for t in list(entries)[:30]]


MANGA_SOURCES = [
    {"name": "Libraccio Fumetti", "category": "fumetti", "source_key": "libraccio",
     "url": "https://www.libraccio.it/reparto/21/fumetti-e-graphic-novels.html?ordinamento=piu-venduti",
     "parser": parse_libraccio, "type": "ranking"},
    {"name": "AnimeClick News", "source_key": "animeclick",
     "url": "https://www.animeclick.it/news", "parser": parse_animeclick_news, "type": "trend"},
]


# ═══════════════════════════════════════════════════════
# SUPABASE INTEGRATION
# ═══════════════════════════════════════════════════════

def push_rankings(supabase_url, supabase_key, chart, entries, chart_date):
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


def push_events(supabase_url, supabase_key, source_key, events):
    """Push eventi locali in external_signal_staging"""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    inserted = 0
    for ev in events:
        row = {
            "feed_key": f"events-{source_key}",
            "source_key": source_key,
            "raw_title": ev.get("title", ""),
            "signal_type": "local_event",
            "signal_text": ev.get("title", ""),
            "event_date": ev.get("event_date"),
            "source_url": ev.get("url", ""),
            "source_payload": json.dumps(ev),
            "processed": False,
        }
        try:
            resp = requests.post(
                f"{supabase_url}/rest/v1/external_signal_staging",
                json=row, headers=headers, timeout=10
            )
            if resp.status_code in (200, 201):
                inserted += 1
        except:
            pass
    return inserted


def push_manga_trends(supabase_url, supabase_key, source_key, trends, signal_type="manga_trend"):
    """Push trend e video in external_signal_staging"""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    inserted = 0
    for t in trends:
        row = {
            "feed_key": f"{signal_type}-{source_key}",
            "source_key": source_key,
            "raw_title": t.get("title", ""),
            "signal_type": signal_type,
            "signal_text": t.get("source_text", t.get("title", "")),
            "source_url": t.get("url", ""),
            "source_payload": json.dumps(t),
            "processed": False,
        }
        try:
            resp = requests.post(
                f"{supabase_url}/rest/v1/external_signal_staging",
                json=row, headers=headers, timeout=10
            )
            if resp.status_code in (200, 201):
                inserted += 1
        except:
            pass
    return inserted


# ═══════════════════════════════════════════════════════
# JSON OUTPUT
# ═══════════════════════════════════════════════════════

def save_json(all_rankings, all_events, all_manga, all_youtube, output_path):
    feed = {
        "source": "Radar completo v11.0",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rankings": [{
            "ean": e.get("ean", ""),
            "title": e.get("title", ""),
            "author": e.get("author", ""),
            "position": e.get("position", 0),
            "category": e.get("_category", ""),
            "source": e.get("_source", ""),
        } for e in all_rankings],
        "local_events": [{
            "title": e.get("title", ""),
            "date": e.get("event_date", ""),
            "url": e.get("url", ""),
            "source": e.get("_source", ""),
        } for e in all_events],
        "manga_trends": [{
            "title": e.get("title", ""),
            "source": e.get("_source", ""),
            "url": e.get("url", ""),
        } for e in all_manga],
        "youtube_trends": [{
            "title": e.get("title", ""),
            "url": e.get("url", ""),
        } for e in all_youtube],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    total = len(feed["rankings"]) + len(feed["local_events"]) + len(feed["manga_trends"]) + len(feed["youtube_trends"])
    print(f"\n✓ JSON salvato: {output_path} ({total} segnali totali)")


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def run_brain_cycle(supabase_url, supabase_key):
    """Esegue il ciclo completo del cervellone dopo lo scraping"""
    print("\n\n━━━ 🧠 CERVELLONE BRAIN CYCLE ━━━\n")
    
    rpc_url = f"{supabase_url}/rest/v1/rpc/daily_brain_cycle"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    
    try:
        resp = requests.post(rpc_url, json={}, headers=headers, timeout=120)
        resp.raise_for_status()
        result = resp.json()
        
        # Estrai stats dal report
        if isinstance(result, list) and len(result) > 0:
            report = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        elif isinstance(result, dict):
            report = result
        elif isinstance(result, str):
            report = json.loads(result)
        else:
            print(f"   ⚠ Risposta inattesa: {type(result)}")
            return
        
        # Ciclo eseguito
        ciclo = report.get("ciclo_eseguito", {})
        if ciclo:
            sync = ciclo.get("sync_segnali", {})
            eventi = ciclo.get("eventi_processati", {})
            recs = ciclo.get("raccomandazioni", {})
            brain = ciclo.get("auto_analisi", {})
            print(f"   Sync segnali: {sync.get('new_signals', '?')} nuovi")
            print(f"   Eventi processati: {eventi.get('events_created', '?')} creati")
            print(f"   Raccomandazioni: {recs.get('inserted_count', '?')} generate")
            print(f"   Auto-analisi: {brain.get('insights_generated', '?')} insight")
        
        # Insight del cervellone
        insights = report.get("cervellone_insights", {})
        if insights:
            print(f"\n   📊 Insight aperti: {insights.get('totale_aperti', '?')}")
            print(f"   ⚠ Warning: {insights.get('warning', 0)}")
            print(f"   💡 Opportunità: {insights.get('opportunity', 0)}")
            top = insights.get("top_actions", [])
            if top:
                print(f"\n   Top azioni:")
                for a in top[:5]:
                    sev = "⚠" if a.get("severity") == "warning" else "💡"
                    print(f"     {sev} {a.get('title', '?')}")
                    print(f"       → {a.get('azione', '?')}")
        
        # Restock urgente
        restock = report.get("restock_urgente", {})
        if restock:
            critici = restock.get("titoli_critici", 0)
            urgenti = restock.get("titoli_urgenti", 0)
            if critici > 0 or urgenti > 0:
                print(f"\n   🚨 Restock: {critici} CRITICI, {urgenti} URGENTI")
        
        # Salute sistema
        salute = report.get("salute_sistema", {})
        if salute:
            print(f"\n   💚 Salute: {salute.get('raccomandazioni_open', '?')} recs, {salute.get('segnali_mercato', '?')} segnali, {salute.get('fonti_attive', '?')} fonti")
        
        print(f"\n   ✓ Brain cycle completato!")
        
    except Exception as e:
        print(f"   ⚠ Brain cycle errore: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supabase-url")
    parser.add_argument("--supabase-key")
    parser.add_argument("--youtube-key")
    parser.add_argument("--output", default="classifiche.json")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    chart_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_rankings = []
    all_events = []
    all_manga = []
    all_youtube = []
    total_ingested = 0
    total_matched = 0
    has_db = args.supabase_url and args.supabase_key and not args.json_only

    print(f"{'='*60}")
    print(f"  RADAR COMPLETO v11.0 — {chart_date}")
    print(f"  Classifiche: {len(RANKING_CHARTS)}")
    print(f"  Eventi locali: {len(LOCAL_EVENT_SOURCES)} fonti")
    print(f"  Manga/anime: {len(MANGA_SOURCES)} fonti")
    print(f"  YouTube API: {'Attiva' if args.youtube_key else 'Inattiva'}")
    print(f"{'='*60}\n")

    # ── CLASSIFICHE ────────────────────────────────────
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
                    total_ingested += r.get("ingested", 0)
                    total_matched += r.get("matched", 0)
                    print(f"   → DB: {r.get('ingested',0)} ingested, {r.get('matched',0)} matched")
            except Exception as e:
                print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    # ── EVENTI LOCALI ──────────────────────────────────
    print("\n\n━━━ EVENTI LOCALI LUNIGIANA ━━━")
    for src in LOCAL_EVENT_SOURCES:
        print(f"\n📍 [{src['source_key'].upper()}] {src['name']}")
        html = fetch_page(src["url"])
        if not html: continue
        events = src["parser"](html)
        print(f"   Trovati {len(events)} eventi")
        if not events: continue
        for e in events:
            e["_source"] = src["source_key"]
        all_events.extend(events)
        if has_db:
            try:
                n = push_events(args.supabase_url, args.supabase_key, src["source_key"], events)
                print(f"   → DB: {n} eventi inseriti in staging")
            except Exception as e:
                print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    # ── MANGA / ANIME TREND ────────────────────────────
    print("\n\n━━━ MANGA / ANIME TREND ━━━")
    for src in MANGA_SOURCES:
        print(f"\n🔥 [{src['source_key'].upper()}] {src['name']}")
        html = fetch_page(src["url"])
        if not html: continue
        
        if src.get("type") == "ranking":
            entries = src["parser"](html)
            print(f"   Trovati {len(entries)} titoli")
            # Già incluso nelle classifiche sopra se è Libraccio Fumetti
        else:
            trends = src["parser"](html)
            print(f"   Trovati {len(trends)} trend/news")
            for t in trends:
                t["_source"] = src["source_key"]
            all_manga.extend(trends)
            if has_db:
                try:
                    n = push_manga_trends(args.supabase_url, args.supabase_key, src["source_key"], trends)
                    print(f"   → DB: {n} trend inseriti in staging")
                except Exception as e:
                    print(f"   ⚠ DB: {e}")
        time.sleep(1.5)

    # ── YOUTUBE BOOKTOK / MANGA ────────────────────────
    print("\n\n━━━ YOUTUBE TREND ━━━")
    if args.youtube_key:
        yt_trends = fetch_youtube_trends(args.youtube_key)
        print(f"   Trovati {len(yt_trends)} video in trend")
        for yt in yt_trends:
            yt["_source"] = "youtube"
        all_youtube.extend(yt_trends)
        if has_db and yt_trends:
            try:
                n = push_manga_trends(args.supabase_url, args.supabase_key, "youtube", yt_trends, signal_type="social_trend")
                print(f"   → DB: {n} video inseriti in staging")
            except Exception as e:
                print(f"   ⚠ DB: {e}")
    else:
        print("   ⚠ YouTube API Key mancante, salto il recupero dei video.")


    # ── BRAIN CYCLE ─────────────────────────────────
    if has_db:
        run_brain_cycle(args.supabase_url, args.supabase_key)

    # ── SALVA JSON ─────────────────────────────────────
    save_json(all_rankings, all_events, all_manga, all_youtube, args.output)

    # ── RIEPILOGO ──────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  RIEPILOGO")
    print(f"  Classifiche: {len(all_rankings)} titoli")
    print(f"  Eventi locali: {len(all_events)} eventi")
    print(f"  Manga trend: {len(all_manga)} segnali")
    print(f"  YouTube trend: {len(all_youtube)} video")
    if total_ingested > 0:
        print(f"  Supabase rankings: {total_ingested} ingested, {total_matched} matched")
    
    from collections import Counter
    print(f"\n  Per fonte:")
    for src, cnt in Counter(e.get("_source","?") for e in all_rankings).most_common():
        print(f"    📊 {src}: {cnt} titoli")
    for src, cnt in Counter(e.get("_source","?") for e in all_events).most_common():
        print(f"    📍 {src}: {cnt} eventi")
    for src, cnt in Counter(e.get("_source","?") for e in all_manga).most_common():
        print(f"    🔥 {src}: {cnt} trend")
    if all_youtube:
        print(f"    ▶️ youtube: {len(all_youtube)} video")
    
    print(f"\n  Done!")


if __name__ == "__main__":
    main()
