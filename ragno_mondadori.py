import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti!")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
mondadori_news = []
headers = {'User-Agent': 'Mozilla/5.0'}

def scansiona_mondadori_doppio():
    uscite_viste = set()
    trovati = 0
    url_main = "https://www.mondadori.it/libri-prossime-uscite/"
    try:
        risp = requests.get(url_main, headers=headers, timeout=15)
        if risp.status_code == 200:
            zuppa = BeautifulSoup(risp.text, 'html.parser')
            titoli_tag = zuppa.find_all(['h2', 'h3', 'a'])
            for t in titoli_tag:
                titolo = t.get_text(strip=True)
                if len(titolo) > 10 and "Scopri" not in titolo and "Leggi tutto" not in titolo and "Mondadori" not in titolo:
                    if titolo not in uscite_viste and trovati < 15:
                        uscite_viste.add(titolo)
                        mondadori_news.append({"signal_text": f"🦅 IN USCITA [Mondadori]: {titolo}", "source_key": "mondadori", "feed_key": "mondadori_releases", "signal_type": "publisher_release"})
                        trovati += 1
    except Exception: pass

    url_oscar = "https://www.oscarmondadori.it/in-arrivo/"
    try:
        risp_oscar = requests.get(url_oscar, headers=headers, timeout=15)
        if risp_oscar.status_code == 200:
            zuppa_oscar = BeautifulSoup(risp_oscar.text, 'html.parser')
            titoli_oscar = zuppa_oscar.find_all(['h3', 'strong'])
            for t in titoli_oscar:
                titolo = t.get_text(strip=True)
                if len(titolo) > 5:
                    if titolo not in uscite_viste and trovati < 30:
                        uscite_viste.add(titolo)
                        mondadori_news.append({"signal_text": f"🗡️ IN USCITA [Oscar Vault]: {titolo}", "source_key": "mondadori", "feed_key": "mondadori_releases", "signal_type": "publisher_release"})
                        trovati += 1
    except Exception: pass

scansiona_mondadori_doppio()
if mondadori_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "mondadori_releases").execute()
        supabase.table("external_signal_staging").insert(mondadori_news).execute()
        print("Salvati.")
    except Exception: pass
