import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti! Impostale come variabili d'ambiente / GitHub Secrets.")
    raise SystemExit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
panini_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("🦸‍♂️ ORACOLO PANINI ATTIVATO: Estrazione delle Prossime Uscite...\n")

def scansiona_panini():
    print("📍 Infiltrazione nei nuovi server di Panini Comics...")
    url = "https://www.panini.it/shp_ita_it/fumetti/calendario-delle-uscite/le-uscite-delle-prossime-8-settimane.html"
    try:
        risposta = requests.get(url, headers=headers, timeout=15)
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            prodotti = zuppa.find_all('a', class_='product-item-link')
            
            uscite_viste = set()
            trovati = 0
            
            for p in prodotti:
                titolo = p.get_text(strip=True)
                if "Abbonamento" in titolo or "Manca il titolo" in titolo:
                    continue
                
                if len(titolo) > 5 and titolo not in uscite_viste and trovati < 25:
                    uscite_viste.add(titolo)
                    
                    panini_news.append({
                        "signal_text": f"🦸‍♂️ IN USCITA: {titolo}",
                        "source_key": "panini-comics-site",
                        "feed_key": "panini_comics_releases",
                        "signal_type": "publisher_release"
                    })
                    trovati += 1
    except Exception as e:
        print(f"   ❌ Errore: {e}")

scansiona_panini()

if panini_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "panini_comics_releases").execute()
        supabase.table("external_signal_staging").insert(panini_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(panini_news)} uscite Panini salvate.")
    except Exception as e:
        pass
