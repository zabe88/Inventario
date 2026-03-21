#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests

USER_AGENT = "TempoPerso-CoverBot/1.0 (+https://github.com/)"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "it-IT,it;q=0.9,en;q=0.6"}
TIMEOUT = 25
RETRY_AFTER_DAYS_DEFAULT = 14
MAX_NEW_DEFAULT = 350
INDEX_PATH_DEFAULT = "index.html"
OUTPUT_PATH_DEFAULT = "covers.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def clean_isbn(raw: str) -> str:
    return re.sub(r"[^0-9Xx]", "", str(raw or "")).upper().strip()


def isbn13to10(isbn13: str) -> str:
    v = clean_isbn(isbn13)
    if len(v) != 13 or not v.startswith('978'):
        return ''
    body = v[3:12]
    total = sum((10 - i) * int(ch) for i, ch in enumerate(body))
    mod = 11 - (total % 11)
    check = 'X' if mod == 10 else ('0' if mod == 11 else str(mod))
    return body + check


def pick_author(raw: str) -> str:
    return clean_spaces(str(raw or '').split(';')[0].split('/')[0].split(',')[0])


def clean_title(raw: str) -> str:
    return clean_spaces(str(raw or '').replace('«', ' ').replace('»', ' ').replace('"', ' '))


def sanitize_image_url(url: str) -> str:
    if not url:
        return ''
    v = str(url).replace('http://', 'https://')
    v = v.replace('&edge=curl', '')
    v = re.sub(r'zoom=\d', 'zoom=2', v)
    return v


def load_dataset_items(index_path: Path) -> List[Dict[str, Any]]:
    raw = index_path.read_text(encoding='utf-8', errors='ignore')
    m = re.search(r"<script[^>]+id=[\"']dataset[\"'][^>]*>(.*?)</script>", raw, re.S)
    if not m:
        raise RuntimeError('Dataset embedded JSON not found in index.html')
    payload = json.loads(m.group(1))
    rows = payload.get('rows', [])
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            giacenti = int(float(r[11] or 0))
        except Exception:
            giacenti = 0
        out.append({
            'id': str(r[0] or ''),
            'titolo': r[1] or '',
            'editore': r[2] or '',
            'fornitore': r[3] or '',
            'volume': r[4] or '',
            'autore': r[5] or '',
            'codice_fornitore': r[6] or '',
            'prezzo': r[7] or '',
            'ean': str(r[8] or '').strip(),
            'posto': r[9] or '',
            'iva': r[10] or '',
            'giacenti': giacenti,
        })
    return out


def build_candidates(items: List[Dict[str, Any]], stock_only: bool = True) -> Dict[str, Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if stock_only and int(item.get('giacenti') or 0) <= 0:
            continue
        isbn13 = clean_isbn(item.get('ean', ''))
        if len(isbn13) not in (10, 13):
            continue
        key = isbn13
        current = seen.get(key)
        author = pick_author(item.get('autore', ''))
        title = clean_title(item.get('titolo', ''))
        enriched = {
            'ean': isbn13,
            'title': title,
            'author': author,
            'publisher': clean_spaces(item.get('editore', '')),
            'stock': int(item.get('giacenti') or 0),
        }
        if current is None:
            seen[key] = enriched
            continue
        if not current.get('author') and author:
            current['author'] = author
        if not current.get('publisher') and enriched['publisher']:
            current['publisher'] = enriched['publisher']
        current['stock'] = max(current.get('stock', 0), enriched['stock'])
    return seen


def load_store(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {'source': 'Tempo Perso Cover Bot', 'updated_at': None, 'catalog_stats': {}, 'covers': {}, 'misses': {}}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {'source': 'Tempo Perso Cover Bot', 'updated_at': None, 'catalog_stats': {}, 'covers': {}, 'misses': {}}
    data.setdefault('source', 'Tempo Perso Cover Bot')
    data.setdefault('updated_at', None)
    data.setdefault('catalog_stats', {})
    data.setdefault('covers', {})
    data.setdefault('misses', {})
    return data


def should_retry_miss(entry: Dict[str, Any], retry_after_days: int) -> bool:
    checked_at = entry.get('checked_at') or entry.get('updated_at')
    if not checked_at:
        return True
    try:
        dt = datetime.fromisoformat(checked_at.replace('Z', '+00:00'))
    except Exception:
        return True
    return datetime.now(timezone.utc) - dt >= timedelta(days=retry_after_days)


def fetch_json(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def image_exists(session: requests.Session, url: str) -> bool:
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        ok = r.status_code == 200 and str(r.headers.get('Content-Type', '')).lower().startswith('image/')
        r.close()
        return ok
    except Exception:
        return False


def try_openlibrary_direct(session: requests.Session, isbn: str) -> Optional[Dict[str, Any]]:
    for size in ('L', 'M'):
        url = f'https://covers.openlibrary.org/b/isbn/{isbn}-{size}.jpg?default=false'
        if image_exists(session, url):
            return {'url': url, 'provider': 'openlibrary_isbn'}
    return None


def try_google_isbn(session: requests.Session, isbn: str) -> Optional[Dict[str, Any]]:
    q = requests.utils.quote('isbn:' + isbn)
    data = fetch_json(session, f'https://www.googleapis.com/books/v1/volumes?q={q}&maxResults=1&printType=books&langRestrict=it')
    item = ((data or {}).get('items') or [None])[0] or {}
    info = item.get('volumeInfo') or {}
    url = sanitize_image_url((info.get('imageLinks') or {}).get('thumbnail') or (info.get('imageLinks') or {}).get('smallThumbnail') or '')
    if url:
        return {'url': url, 'provider': 'google_isbn'}
    return None


def try_openlibrary_search(session: requests.Session, title: str, author: str) -> Optional[Dict[str, Any]]:
    if not title:
        return None
    params = {'title': title, 'limit': '5'}
    if author:
        params['author'] = author
    url = 'https://openlibrary.org/search.json'
    try:
        r = session.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None
    for doc in data.get('docs', [])[:5]:
        cover_i = doc.get('cover_i')
        if cover_i:
            return {'url': f'https://covers.openlibrary.org/b/id/{cover_i}-M.jpg', 'provider': 'openlibrary_search'}
    return None


def try_google_title(session: requests.Session, title: str, author: str) -> Optional[Dict[str, Any]]:
    if not title:
        return None
    parts = ['intitle:' + title]
    if author:
        parts.append('inauthor:' + author)
    q = requests.utils.quote(' '.join(parts))
    data = fetch_json(session, f'https://www.googleapis.com/books/v1/volumes?q={q}&maxResults=3&printType=books&langRestrict=it')
    for item in (data or {}).get('items', [])[:3]:
        info = item.get('volumeInfo') or {}
        url = sanitize_image_url((info.get('imageLinks') or {}).get('thumbnail') or (info.get('imageLinks') or {}).get('smallThumbnail') or '')
        if url:
            return {'url': url, 'provider': 'google_title'}
    return None


def resolve_cover(session: requests.Session, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    isbn13 = clean_isbn(candidate.get('ean', ''))
    isbn10 = isbn13to10(isbn13)
    title = clean_title(candidate.get('title', ''))
    author = pick_author(candidate.get('author', ''))
    attempts = []
    if isbn13:
        attempts.append(lambda: try_openlibrary_direct(session, isbn13))
    if isbn10:
        attempts.append(lambda: try_openlibrary_direct(session, isbn10))
    if isbn13:
        attempts.append(lambda: try_google_isbn(session, isbn13))
    if isbn10:
        attempts.append(lambda: try_google_isbn(session, isbn10))
    if title:
        attempts.append(lambda: try_openlibrary_search(session, title, author))
    if title:
        attempts.append(lambda: try_google_title(session, title, author))
    for attempt in attempts:
        found = attempt()
        if found and found.get('url'):
            return found
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description='Build covers.json for Tempo Perso inventory')
    parser.add_argument('--index', default=INDEX_PATH_DEFAULT)
    parser.add_argument('--output', default=OUTPUT_PATH_DEFAULT)
    parser.add_argument('--max-new', type=int, default=MAX_NEW_DEFAULT)
    parser.add_argument('--retry-after-days', type=int, default=RETRY_AFTER_DAYS_DEFAULT)
    parser.add_argument('--include-zero-stock', action='store_true')
    args = parser.parse_args()

    index_path = Path(args.index)
    output_path = Path(args.output)

    items = load_dataset_items(index_path)
    candidates = build_candidates(items, stock_only=not args.include_zero_stock)
    store = load_store(output_path)
    covers = store.get('covers', {})
    misses = store.get('misses', {})

    # Remove stale entries that are no longer in the catalog.
    valid_keys = set(candidates.keys())
    covers = {k: v for k, v in covers.items() if k in valid_keys}
    misses = {k: v for k, v in misses.items() if k in valid_keys}

    queue: List[Dict[str, Any]] = []
    for key, cand in candidates.items():
        if key in covers:
            continue
        miss = misses.get(key)
        if miss and not should_retry_miss(miss, args.retry_after_days):
            continue
        queue.append(cand)
    queue.sort(key=lambda x: (-int(x.get('stock') or 0), x.get('title') or ''))
    queue = queue[: max(args.max_new, 0)]

    session = requests.Session()
    processed = 0
    found_now = 0
    checked_at = now_iso()
    for cand in queue:
        key = cand['ean']
        result = resolve_cover(session, cand)
        processed += 1
        if result:
            covers[key] = {
                'url': result['url'],
                'provider': result['provider'],
                'checked_at': checked_at,
                'title': cand.get('title', ''),
                'author': cand.get('author', ''),
                'publisher': cand.get('publisher', ''),
            }
            if key in misses:
                misses.pop(key, None)
            found_now += 1
        else:
            misses[key] = {
                'checked_at': checked_at,
                'title': cand.get('title', ''),
                'author': cand.get('author', ''),
                'publisher': cand.get('publisher', ''),
                'status': 'miss'
            }
        time.sleep(0.08)

    pending = 0
    for key in candidates.keys():
        if key in covers:
            continue
        miss = misses.get(key)
        if not miss or should_retry_miss(miss, args.retry_after_days):
            pending += 1

    store['updated_at'] = checked_at
    store['source'] = 'Tempo Perso Cover Bot'
    store['covers'] = dict(sorted(covers.items()))
    store['misses'] = dict(sorted(misses.items()))
    store['catalog_stats'] = {
        'source_items_with_stock': sum(1 for x in items if int(x.get('giacenti') or 0) > 0),
        'unique_isbn_candidates': len(candidates),
        'with_cover': len(covers),
        'misses': len(misses),
        'pending': pending,
        'processed_this_run': processed,
        'found_this_run': found_now,
        'max_new_this_run': args.max_new,
    }
    output_path.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(store['catalog_stats'], ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
