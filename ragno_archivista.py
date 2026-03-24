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
    
    # 1. Ricerca globale per Titolo (perfetta per i libri locali/indipendenti)
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
    # 1. Vediamo quali copertine abbiamo già trovato per non fare lavoro doppio
    res_covers = supabase.table('external_signal_staging').select('feed_key').eq('signal_type', 'book_cover').execute()
    ean_con_cover = [r['feed_key'] for r in res_covers.data] if res_covers.data else []
    
    # 2. Guardiamo l'inventario
    res_libri = supabase.table('assistant_triage_inventario').select('ean, titolo').execute()
    tutti_i_libri = res_libri.data if res_libri.data else []
    
    # 3. Troviamo i "fantasmi" (libri senza copertina registrata)
    fantasmi = [b for b in tutti_i_libri if b.get('ean') and str(b.get('ean')) not in ean_con_cover]
    
    # Elaboriamo 30 libri a notte per non farci bloccare da Google
    fantasmi_da_cercare = fantasmi[:30]
    
    if not fantasmi_da_cercare:
        print("Nessun fantasma trovato. Il magazzino è in ordine.")
        return

    print(f"Trovati {len(fantasmi)} libri senza identità. Ne caccio {len(fantasmi_da_cercare)} stanotte...")
    
    nuove_cover = []
    for libro in fantasmi_da_cercare:
        ean = str(libro['ean'])
        titolo = libro['titolo']
        print(f"  🔍 Indago su: {titolo} ({ean})")
        
        url = trova_copertina(ean, titolo)
        
        if url:
            nuove_cover.append({"signal_text": url, "source_key": "archivista", "feed_key": ean, "signal_type": "book_cover"})
            print(f"     ✅ Identità confermata: {url}")
        else:
            # Salviamo un "NOT_FOUND" così stanotte non lo cerchiamo di nuovo inutilmente
            nuove_cover.append({"signal_text": "NOT_FOUND", "source_key": "archivista", "feed_key": ean, "signal_type": "book_cover"})
            print("     ❌ Fantasma assoluto.")
            
        time.sleep(1.5) # Pausa tattica per non sembrare un bot
        
    if nuove_cover:
        supabase.table('external_signal_staging').insert(nuove_cover).execute()
        print(f"Ricerca finita. {len(nuove_cover)} file chiusi.")

scansiona_magazzino()
