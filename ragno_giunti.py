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
giunti_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("📚 ORACOLO GIUNTI ATTIVATO: Estrazione delle Novità...\n")

def scansiona_giunti():
    print("📍 Infiltrazione nell'e-commerce di Giunti Editore...")
    url = "https://giunti.it/pages/ultime-uscite"
    try:
        risposta = requests.get(url, headers=headers, timeout=15)
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            prodotti = zuppa.find_all('a', href=True)
            
            uscite_viste = set()
            trovati = 0
            
            for p in prodotti:
                link = p['href']
                titolo = p.get_text(strip=True)
                
                # Il Filtro Magico: escludiamo i testi fusi con "Prezzo" e "Aggiungi"
                if "/products/" in link and len(titolo) > 3 and "Aggiungi" not in titolo and "Prezzo" not in titolo:
                    
                    if titolo not in uscite_viste and trovati < 25:
                        uscite_viste.add(titolo)
                        
                        giunti_news.append({
                            "signal_text": f"📚 IN USCITA: {titolo}",
                            "source_key": "giunti",
                            "feed_key": "giunti_releases",
                            "signal_type": "publisher_release"
                        })
                        trovati += 1
    except Exception as e:
        print(f"   ❌ Errore: {e}")

scansiona_giunti()

if giunti_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "giunti_releases").execute()
        supabase.table("external_signal_staging").insert(giunti_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(giunti_news)} novità Giunti Editore immagazzinate.")
    except Exception as e:
        pass
