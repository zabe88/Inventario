import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import re


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti! Impostale come variabili d'ambiente / GitHub Secrets.")
    raise SystemExit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
star_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("⭐ ORACOLO STAR COMICS ATTIVATO: Intercettazione del Calendario Uscite...\n")

def scansiona_star():
    print("📍 Scassinando la cassaforte di Star Comics...")
    url = "https://www.starcomics.com/uscite"
    try:
        risposta = requests.get(url, headers=headers, timeout=10)
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            regex_data = re.compile(r'\d{2}/\d{2}/\d{4}')
            contenitori = zuppa.find_all(['div', 'li', 'span', 'p'])
            
            uscite_viste = set()
            trovati = 0
            
            for c in contenitori:
                testo = c.get_text(separator=" ", strip=True)
                if regex_data.search(testo) and '€' in testo and 10 < len(testo) < 150:
                    testo_pulito = testo.replace('\n', ' ').replace('  ', ' ')
                    
                    if testo_pulito not in uscite_viste and trovati < 20:
                        uscite_viste.add(testo_pulito)
                        
                        star_news.append({
                            "signal_text": f"⭐ IN USCITA: {testo_pulito}",
                            "source_key": "star-comics-site",
                            "feed_key": "star_comics_releases",
                            "signal_type": "publisher_release"
                        })
                        trovati += 1
    except Exception as e:
        print(f"   ❌ Errore: {e}")

scansiona_star()

if star_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "star_comics_releases").execute()
        supabase.table("external_signal_staging").insert(star_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(star_news)} uscite Star Comics salvate.")
    except Exception as e:
        pass
