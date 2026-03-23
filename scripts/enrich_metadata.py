#!/usr/bin/env python3
"""
Arricchimento metadati titoli via OpenLibrary + Google Books (gratis, no API key).
v2: aggiunge Google Books come fallback — copertura molto migliore per editoria italiana.

Uso: python enrich_metadata.py --supabase-url URL --supabase-key KEY [--batch 200]
"""

import argparse
import json
import re
import time
import requests

HEADERS = {
    "User-Agent": "CervelloneLibreria/2.0 (pontremoli bookstore inventory enrichment)"
}


# ═══════════════════════════════════════════════════════
# GOOGLE BOOKS (gratis, no API key, 1000 req/giorno)
# ═══════════════════════════════════════════════════════

def fetch_google_books(isbn: str) -> dict | None:
    """Cerca un ISBN su Google Books API (gratis, no key)"""
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&maxResults=1"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("totalItems", 0) > 0:
                return data["items"][0].get("volumeInfo", {})
        return None
    except:
        return None


def parse_google_books(vol: dict) -> dict:
    """Parsa i dati da Google Books volumeInfo"""
    result = {
        "title_original": vol.get("title", ""),
        "authors": vol.get("authors", []),
        "publisher_original": vol.get("publisher"),
        "publish_year": None,
        "page_count": vol.get("pageCount"),
        "language": vol.get("language"),
        "subjects": vol.get("categories", []),
        "description": (vol.get("description") or "")[:2000],
        "cover_url": None,
        "series_name": None,
        "volume_number": None,
        "isbn_10": None,
        "isbn_13": None,
        "openlibrary_key": None,
    }
    
    # Anno
    pub_date = vol.get("publishedDate", "")
    if pub_date:
        year_match = re.search(r'(19|20)\d{2}', pub_date)
        if year_match:
            result["publish_year"] = int(year_match.group(0))
    
    # ISBN
    for ident in vol.get("industryIdentifiers", []):
        if ident.get("type") == "ISBN_10":
            result["isbn_10"] = ident["identifier"]
        elif ident.get("type") == "ISBN_13":
            result["isbn_13"] = ident["identifier"]
    
    # Copertina
    images = vol.get("imageLinks", {})
    result["cover_url"] = images.get("thumbnail") or images.get("smallThumbnail")
    
    return result


# ═══════════════════════════════════════════════════════
# OPENLIBRARY (gratis, no limit, copertura internazionale)
# ═══════════════════════════════════════════════════════

def fetch_openlibrary(isbn: str) -> dict | None:
    """Cerca un ISBN su OpenLibrary"""
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return None
    except:
        return None


def fetch_ol_work(work_key: str) -> dict | None:
    url = f"https://openlibrary.org{work_key}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


def fetch_ol_author(author_key: str) -> str | None:
    url = f"https://openlibrary.org{author_key}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            return d.get("name") or d.get("personal_name")
        return None
    except:
        return None


def parse_openlibrary(edition: dict) -> dict:
    """Parsa i dati da OpenLibrary edition"""
    result = {
        "openlibrary_key": edition.get("key", ""),
        "title_original": edition.get("title", ""),
        "publish_year": None,
        "page_count": edition.get("number_of_pages"),
        "language": None,
        "isbn_10": None,
        "isbn_13": None,
        "publisher_original": None,
        "authors": [],
        "subjects": [],
        "description": None,
        "cover_url": None,
        "series_name": None,
        "volume_number": None,
    }
    
    pub_date = edition.get("publish_date", "")
    if pub_date:
        year_match = re.search(r'(19|20)\d{2}', pub_date)
        if year_match:
            result["publish_year"] = int(year_match.group(0))
    
    langs = edition.get("languages", [])
    if langs:
        lang_key = langs[0].get("key", "")
        lang_map = {"/languages/ita": "it", "/languages/eng": "en", "/languages/fre": "fr",
                    "/languages/ger": "de", "/languages/spa": "es", "/languages/jpn": "ja"}
        result["language"] = lang_map.get(lang_key, lang_key.split("/")[-1])
    
    isbn_10_list = edition.get("isbn_10", [])
    isbn_13_list = edition.get("isbn_13", [])
    if isbn_10_list: result["isbn_10"] = isbn_10_list[0]
    if isbn_13_list: result["isbn_13"] = isbn_13_list[0]
    
    publishers = edition.get("publishers", [])
    if publishers: result["publisher_original"] = publishers[0]
    
    covers = edition.get("covers", [])
    if covers:
        result["cover_url"] = f"https://covers.openlibrary.org/b/id/{covers[0]}-M.jpg"
    
    series = edition.get("series", [])
    if series:
        result["series_name"] = series[0] if isinstance(series[0], str) else str(series[0])
    
    # Autori
    authors_refs = edition.get("authors", [])
    for a in authors_refs[:3]:
        a_key = a.get("key", "")
        if a_key:
            name = fetch_ol_author(a_key)
            if name:
                result["authors"].append(name)
            time.sleep(0.3)
    
    # Work → soggetti e descrizione
    works = edition.get("works", [])
    if works:
        work_key = works[0].get("key", "")
        if work_key:
            work = fetch_ol_work(work_key)
            if work:
                subjects = work.get("subjects", [])
                result["subjects"] = subjects[:20] if subjects else []
                desc = work.get("description", "")
                if isinstance(desc, dict):
                    desc = desc.get("value", "")
                if desc:
                    result["description"] = desc[:2000]
    
    return result


# ═══════════════════════════════════════════════════════
# ENRICHMENT PIPELINE: Google Books first, OpenLibrary fallback
# ═══════════════════════════════════════════════════════

def enrich_isbn(isbn: str) -> dict | None:
    """Cerca prima su Google Books (migliore per IT), poi OpenLibrary"""
    
    # 1. Google Books
    gb = fetch_google_books(isbn)
    if gb and gb.get("title"):
        meta = parse_google_books(gb)
        meta["source"] = "google_books"
        return meta
    
    time.sleep(0.5)
    
    # 2. OpenLibrary fallback
    ol = fetch_openlibrary(isbn)
    if ol and ol.get("title"):
        meta = parse_openlibrary(ol)
        meta["source"] = "openlibrary"
        return meta
    
    return None


# ═══════════════════════════════════════════════════════
# SUPABASE
# ═══════════════════════════════════════════════════════

def get_titles_to_enrich(supabase_url, supabase_key, batch_size):
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"}
    resp = requests.get(
        f"{supabase_url}/rest/v1/titles_to_enrich?limit={batch_size}",
        headers=headers, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def push_metadata(supabase_url, supabase_key, item_id, ean, metadata):
    headers = {
        "apikey": supabase_key, "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json", "Prefer": "return=minimal",
    }
    row = {
        "item_id": str(item_id), "ean": ean,
        "title_original": metadata.get("title_original"),
        "authors": json.dumps(metadata.get("authors", [])),
        "publisher_original": metadata.get("publisher_original"),
        "publish_year": metadata.get("publish_year"),
        "page_count": metadata.get("page_count"),
        "language": metadata.get("language"),
        "subjects": json.dumps(metadata.get("subjects", [])),
        "description": metadata.get("description"),
        "cover_url": metadata.get("cover_url"),
        "series_name": metadata.get("series_name"),
        "volume_number": metadata.get("volume_number"),
        "isbn_10": metadata.get("isbn_10"),
        "isbn_13": metadata.get("isbn_13"),
        "openlibrary_key": metadata.get("openlibrary_key"),
        "source": metadata.get("source", "unknown"),
    }
    resp = requests.post(f"{supabase_url}/rest/v1/title_metadata", json=row, headers=headers, timeout=10)
    return resp.status_code in (200, 201)


def push_not_found(supabase_url, supabase_key, item_id, ean):
    headers = {
        "apikey": supabase_key, "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json", "Prefer": "return=minimal",
    }
    row = {"item_id": str(item_id), "ean": ean, "source": "not_found"}
    resp = requests.post(f"{supabase_url}/rest/v1/title_metadata", json=row, headers=headers, timeout=10)
    return resp.status_code in (200, 201)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supabase-url", required=True)
    parser.add_argument("--supabase-key", required=True)
    parser.add_argument("--batch", type=int, default=200)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ARRICCHIMENTO METADATI v2")
    print(f"  Google Books + OpenLibrary")
    print(f"  Batch: {args.batch} titoli")
    print(f"{'='*60}\n")

    titles = get_titles_to_enrich(args.supabase_url, args.supabase_key, args.batch)
    print(f"Titoli da arricchire: {len(titles)}\n")

    enriched = 0
    not_found = 0
    errors = 0
    by_source = {"google_books": 0, "openlibrary": 0}

    for i, t in enumerate(titles):
        ean = t["ean"]
        titolo = t.get("titolo", "?")[:50]
        print(f"  [{i+1}/{len(titles)}] {ean} — {titolo}...", end=" ")
        
        try:
            meta = enrich_isbn(ean)
            if meta and meta.get("title_original"):
                ok = push_metadata(args.supabase_url, args.supabase_key, t["item_id"], ean, meta)
                if ok:
                    enriched += 1
                    src = meta.get("source", "?")
                    by_source[src] = by_source.get(src, 0) + 1
                    subj = len(meta.get("subjects", []))
                    print(f"✓ [{src[:2].upper()}] {meta.get('language','?')} | {meta.get('publish_year','?')} | {subj} sogg.")
                else:
                    errors += 1
                    print("⚠ DB error")
            else:
                push_not_found(args.supabase_url, args.supabase_key, t["item_id"], ean)
                not_found += 1
                print("— non trovato")
        except Exception as e:
            errors += 1
            print(f"⚠ {e}")
        
        time.sleep(1.0)

    print(f"\n{'='*60}")
    print(f"  RISULTATO")
    print(f"  Arricchiti: {enriched}")
    for src, cnt in by_source.items():
        if cnt > 0: print(f"    {src}: {cnt}")
    print(f"  Non trovati: {not_found}")
    print(f"  Errori: {errors}")
    print(f"  Processati: {enriched + not_found + errors}/{len(titles)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
