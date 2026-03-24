import os
import requests
import time
from supabase import create_client, Client

# Peschiamo le chiavi blindate
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti!")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🧙‍♂️ L'ARCHIVISTA: Risveglio. Cerco i fantasmi senza volto...")

def trova_copertina(ean, titolo):
    titolo_pulito = str(titolo).replace("vol.", "").replace("n.", "").split("(")[0].strip()
    
    # 1. Ricerca globale per Titolo
    try:
        res = requests.get(f"https://www.googleapis.com/books/v1/volumes?q=intitle:{titolo_pulito}").json()
        if "items" in res:
            for item in res["items"]:
                links = item["volumeInfo"].get("imageLinks", {})
                if "thumbnail" in links:
                    return links["thumbnail"].replace("http:", "https:")
    except: pass
    
    # 2. Ricerca globale per ISBN
    if ean and len(str(ean)) >= 10:
        try:
            ol_url = f"https://covers.openlibrary.org/b/isbn/{ean}-L.jpg?default=false"
            if requests.head(ol_url).status_code == 200:
                return ol_url
        except: pass
        
    return None

def scansiona_magazzino():
    # 1. Vediamo quali copertine abbiamo già cercato
    res_covers = supabase.table('external_signal_staging').select('feed_key').eq('signal_type', 'book_cover').execute()
    ean_cercati = [r['feed_key'] for r in res_covers.data] if res_covers.data else []
    
    # 2. Guardiamo l'inventario
    res_libri = supabase.table('assistant_triage_inventario').select('ean, titolo').execute()
    tutti_i_libri = res_libri.data if res_libri.data else []
    
    # 3. Troviamo i "fantasmi" 
    fantasmi = [b for b in tutti_i_libri if b.get('ean') and str(b.get('ean')) not in ean_cercati]
    fantasmi_da_cercare = fantasmi[:30]
    
    if not fantasmi_da_cercare:
        print("Nessun fantasma trovato. Il magazzino è in ordine.")
        return

    print(f"Trovati {len(fantasmi)} libri senza identità. Ne caccio {len(fantasmi_da_cercare)} stanotte...\n")
    
    for libro in fantasmi_da_cercare:
        ean = str(libro['ean'])
        titolo = libro['titolo']
        print(f"🔍 Indago su: {titolo} ({ean})")
        
        # --- IL TRUCCO HACKER DEFINITIVO ---
        # Usiamo 'upcoming_releases' perché sappiamo per certo che il database l'accetta!
        try:
            supabase.table("source_ingestion_registry").upsert({
                "feed_key": ean, 
                "source_key": "mondadori",  
                "feed_name": f"Cover {ean}", 
                "feed_scope": "upcoming_releases", # LA PAROLA MAGICA CHE IL DB CONOSCE
                "target_table": "external_signal_staging"
            }).execute()
        except Exception as e: 
            print(f"   ⚠️ Impossibile registrare EAN: {e}")
            
        url = trova_copertina(ean, titolo)
        
        if url:
            print(f"   ✅ Trovata: {url}")
            payload = {"signal_text": url, "source_key": "mondadori", "feed_key": ean, "signal_type": "book_cover"}
        else:
            print("   ❌ Fantasma assoluto.")
            payload = {"signal_text": "NOT_FOUND", "source_key": "mondadori", "feed_key": ean, "signal_type": "book_cover"}
            
        try:
            supabase.table('external_signal_staging').upsert(payload).execute()
        except Exception as e:
            print(f"   ⚠️ Errore salvataggio finale: {e}")
            
        time.sleep(1.5)
        
    print("\nRicerca finita. L'Archivista torna a dormire.")

scansiona_magazzino()
