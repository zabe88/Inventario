import os
import requests
import time
import re
from supabase import create_client, Client

# Peschiamo le chiavi blindate
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti!")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🧙‍♂️ L'ARCHIVISTA: PURGA TOTALE INIZIATA. Non si dorme finché non finiamo.")

def pulisci_titolo_per_google(titolo):
    # Rimuoviamo le "sporcizie" da fumetteria che confondono i database mondiali
    t = str(titolo).lower()
    t = re.sub(r'vol\.?\s*\d+', '', t) # Toglie "vol. 109"
    t = re.sub(r'n\.?\s*\d+', '', t)   # Toglie "n. 109"
    t = t.replace("new edition", "").replace("variant", "").replace("limited", "").replace("ed.", "")
    t = t.split("(")[0].strip() # Toglie roba tra parentesi
    return t

def trova_copertina(ean, titolo):
    titolo_pulito = pulisci_titolo_per_google(titolo)
    
    # 1. Ricerca globale per ISBN
    if ean and len(str(ean)) >= 10:
        try:
            ol_url = f"https://covers.openlibrary.org/b/isbn/{ean}-L.jpg?default=false"
            if requests.head(ol_url).status_code == 200:
                return ol_url
        except: pass
        
    # 2. Ricerca globale per Titolo (pulito dai termini manga)
    try:
        res = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=intitle:{titolo_pulito}").json()
        if "items" in res:
            for item in res["items"]:
                links = item["volumeInfo"].get("imageLinks", {})
                if "thumbnail" in links:
                    return links["thumbnail"].replace("http:", "https:")
    except: pass
        
    return None

def scansiona_magazzino():
    res_covers = supabase.table('external_signal_staging').select('feed_key').eq('signal_type', 'book_cover').execute()
    ean_cercati = [r['feed_key'] for r in res_covers.data] if res_covers.data else []
    
    res_libri = supabase.table('assistant_triage_inventario').select('ean, titolo').execute()
    tutti_i_libri = res_libri.data if res_libri.data else []
    
    fantasmi = [b for b in tutti_i_libri if b.get('ean') and str(b.get('ean')) not in ean_cercati]
    
    # IL GUINZAGLIO È TOLTO: 1000 libri invece di 30!
    fantasmi_da_cercare = fantasmi[:1000]
    
    if not fantasmi_da_cercare:
        print("Nessun fantasma trovato. Il magazzino è in ordine perfetto.")
        return

    print(f"Trovati {len(fantasmi)} libri senza identità. Ne caccio {len(fantasmi_da_cercare)} IN UNA SOLA VOLTA...\n")
    
    for i, libro in enumerate(fantasmi_da_cercare):
        ean = str(libro['ean'])
        titolo = libro['titolo']
        print(f"[{i+1}/{len(fantasmi_da_cercare)}] 🔍 Indago su: {titolo}")
        
        try:
            supabase.table("source_ingestion_registry").upsert({
                "feed_key": ean, "source_key": "mondadori", "feed_name": f"Cover {ean}", 
                "feed_scope": "upcoming_releases", "target_table": "external_signal_staging"
            }).execute()
        except Exception: pass
            
        url = trova_copertina(ean, titolo)
        
        if url:
            print(f"   ✅ Trovata: {url}")
            payload = {"signal_text": url, "source_key": "mondadori", "feed_key": ean, "signal_type": "book_cover"}
        else:
            print("   ❌ Fantasma.")
            payload = {"signal_text": "NOT_FOUND", "source_key": "mondadori", "feed_key": ean, "signal_type": "book_cover"}
            
        try:
            supabase.table('external_signal_staging').upsert(payload).execute()
        except Exception as e:
            print(f"   ⚠️ Errore salvataggio: {e}")
            
        time.sleep(1.2) # Pausa leggermente ridotta per fare prima
        
    print("\nPURGA FINITA. Tutti i libri processati.")

scansiona_magazzino()
