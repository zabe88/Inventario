#!/usr/bin/env python3
"""
Arricchimento metadati titoli via OpenLibrary (gratuito, no API key).
Processa batch di titoli con EAN, arricchisce con: autori, anno, pagine,
soggetti, descrizione, copertina, lingua, serie.

Uso: python enrich_metadata.py --supabase-url URL --supabase-key KEY [--batch 100]
"""

import argparse
import json
import time
import requests

HEADERS = {
    "User-Agent": "CervelloneLibreria/1.0 (pontremoli bookstore inventory enrichment)"
}

def fetch_openlibrary_isbn(isbn: str) -> dict | None:
    """Cerca un ISBN su OpenLibrary Books API"""
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return None
    except:
        return None


def fetch_openlibrary_work(work_key: str) -> dict | None:
    """Recupera dettagli del work (soggetti, descrizione)"""
    url = f"https://openlibrary.org{work_key}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return None
    except:
        return None


def fetch_author_name(author_key: str) -> str | None:
    """Recupera nome autore"""
    url = f"https://openlibrary.org{author_key}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("name") or data.get("personal_name")
        return None
    except:
        return None


def enrich_isbn(isbn: str) -> dict | None:
    """Arricchisce un ISBN con tutti i metadati disponibili"""
    edition = fetch_openlibrary_isbn(isbn)
    if not edition:
        return None
    
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
    
    # Anno pubblicazione
    pub_date = edition.get("publish_date", "")
    if pub_date:
        import re
        year_match = re.search(r'(19|20)\d{2}', pub_date)
        if year_match:
            result["publish_year"] = int(year_match.group(0))
    
    # Lingua
    langs = edition.get("languages", [])
    if langs:
        lang_key = langs[0].get("key", "")
        lang_map = {"/languages/ita": "it", "/languages/eng": "en", "/languages/fre": "fr",
                    "/languages/ger": "de", "/languages/spa": "es", "/languages/jpn": "ja"}
        result["language"] = lang_map.get(lang_key, lang_key.split("/")[-1])
    
    # ISBN
    isbn_10_list = edition.get("isbn_10", [])
    isbn_13_list = edition.get("isbn_13", [])
    if isbn_10_list: result["isbn_10"] = isbn_10_list[0]
    if isbn_13_list: result["isbn_13"] = isbn_13_list[0]
    
    # Editore
    publishers = edition.get("publishers", [])
    if publishers: result["publisher_original"] = publishers[0]
    
    # Copertina
    covers = edition.get("covers", [])
    if covers:
        result["cover_url"] = f"https://covers.openlibrary.org/b/id/{covers[0]}-M.jpg"
    
    # Serie
    series = edition.get("series", [])
    if series:
        result["series_name"] = series[0] if isinstance(series[0], str) else str(series[0])
    
    # Autori (da edition)
    authors_refs = edition.get("authors", [])
    author_names = []
    for a in authors_refs[:3]:  # max 3 autori
        a_key = a.get("key", "")
        if a_key:
            name = fetch_author_name(a_key)
            if name:
                author_names.append(name)
            time.sleep(0.3)
    result["authors"] = author_names
    
    # Work → soggetti e descrizione
    works = edition.get("works", [])
    if works:
        work_key = works[0].get("key", "")
        if work_key:
            work = fetch_openlibrary_work(work_key)
            if work:
                # Soggetti
                subjects = work.get("subjects", [])
                result["subjects"] = subjects[:20] if subjects else []
                
                # Descrizione
                desc = work.get("description", "")
                if isinstance(desc, dict):
                    desc = desc.get("value", "")
                if desc:
                    result["description"] = desc[:2000]
    
    return result


def get_titles_to_enrich(supabase_url, supabase_key, batch_size):
    """Recupera titoli da arricchire"""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }
    resp = requests.get(
        f"{supabase_url}/rest/v1/titles_to_enrich?limit={batch_size}",
        headers=headers, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def push_metadata(supabase_url, supabase_key, item_id, ean, metadata):
    """Salva metadati in title_metadata"""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    row = {
        "item_id": str(item_id),
        "ean": ean,
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
        "source": "openlibrary",
    }
    resp = requests.post(
        f"{supabase_url}/rest/v1/title_metadata",
        json=row, headers=headers, timeout=10
    )
    return resp.status_code in (200, 201)


def push_not_found(supabase_url, supabase_key, item_id, ean):
    """Segna come non trovato (così non riprova)"""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    row = {
        "item_id": str(item_id),
        "ean": ean,
        "source": "openlibrary_notfound",
    }
    resp = requests.post(
        f"{supabase_url}/rest/v1/title_metadata",
        json=row, headers=headers, timeout=10
    )
    return resp.status_code in (200, 201)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--supabase-url", required=True)
    parser.add_argument("--supabase-key", required=True)
    parser.add_argument("--batch", type=int, default=100)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ARRICCHIMENTO METADATI via OpenLibrary")
    print(f"  Batch: {args.batch} titoli")
    print(f"{'='*60}\n")

    titles = get_titles_to_enrich(args.supabase_url, args.supabase_key, args.batch)
    print(f"Titoli da arricchire in questo batch: {len(titles)}\n")

    enriched = 0
    not_found = 0
    errors = 0

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
                    subj = len(meta.get("subjects", []))
                    print(f"✓ {meta.get('language','?')} | {meta.get('publish_year','?')} | {subj} sogg.")
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
        
        # Rate limit: OpenLibrary chiede max 1 req/sec
        time.sleep(1.2)

    print(f"\n{'='*60}")
    print(f"  RISULTATO")
    print(f"  Arricchiti: {enriched}")
    print(f"  Non trovati: {not_found}")
    print(f"  Errori: {errors}")
    print(f"  Totale processati: {enriched + not_found + errors}/{len(titles)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
