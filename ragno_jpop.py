import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# CHIAVI SEGRETE PESCATE DA GITHUB SECRETS
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRORE: Chiavi Supabase mancanti! Assicurati di averle impostate nei Secrets di GitHub.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
jpop_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

def scansiona_jpop():
    url = "https://j-pop.it/it/blog/uscitedellasettima"
    try:
        risposta = requests.get(url, headers=headers, timeout=15)
        uscite_viste = set()
        trovati = 0
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            prodotti = zuppa.find_all('a')
            for p in prodotti:
                titolo = p.get_text(strip=True)
                if "uscite della settimana" in titolo.lower() and titolo not in uscite_viste and trovati < 10:
                    uscite_viste.add(titolo)
                    jpop_news.append({"signal_text": f"🌸 ALERT J-POP: {titolo}", "source_key": "jpop", "feed_key": "jpop_releases", "signal_type": "publisher_release"})
                    trovati += 1
            if trovati == 0:
                 url_home = "https://j-pop.it/"
                 risp = requests.get(url_home, headers=headers)
                 zuppa_home = BeautifulSoup(risp.text, 'html.parser')
                 volumi = zuppa_home.find_all('a', class_='product-item-link')
                 for v in volumi:
                     tit_vol = v.get_text(strip=True)
                     if tit_vol not in uscite_viste and trovati < 20:
                         uscite_viste.add(tit_vol)
                         jpop_news.append({"signal_text": f"🌸 IN USCITA [J-Pop]: {tit_vol}", "source_key": "jpop", "feed_key": "jpop_releases", "signal_type": "publisher_release"})
                         trovati += 1
    except Exception as e:
        print(f"Errore: {e}")

scansiona_jpop()
if jpop_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "jpop_releases").execute()
        supabase.table("external_signal_staging").insert(jpop_news).execute()
        print(f"Vittoria. {len(jpop_news)} salvati.")
    except Exception as e:
        pass
