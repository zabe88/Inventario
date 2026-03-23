import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- LE CHIAVI DEL TUO CERVELLONE ---
SUPABASE_URL = "https://aacqebirvnkrbewvgmvo.supabase.co"
SUPABASE_KEY = "sb_publishable_opJ7oXwCxaT53ym88mSxOA_-iNxA1f3"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

jpop_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("🌸 ORACOLO J-POP V2: Ricalibrazione sui nuovi server...\n")

def scansiona_jpop():
    print("📍 Infiltrazione nella bacheca segreta settimanale...")
    url = "https://j-pop.it/it/blog/uscitedellasettima"
    try:
        risposta = requests.get(url, headers=headers, timeout=15)
        uscite_viste = set()
        trovati = 0
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            prodotti = zuppa.find_all('a')
            
            # PIANO A: Post settimanali
            for p in prodotti:
                titolo = p.get_text(strip=True)
                if "uscite della settimana" in titolo.lower() and titolo not in uscite_viste and trovati < 10:
                    uscite_viste.add(titolo)
                    jpop_news.append({
                        "signal_text": f"🌸 ALERT J-POP: {titolo}",
                        "source_key": "jpop",
                        "feed_key": "jpop_releases",
                        "signal_type": "publisher_release"
                    })
                    trovati += 1
                    
            # PIANO B: Catalogo
            if trovati == 0:
                 url_home = "https://j-pop.it/"
                 risp = requests.get(url_home, headers=headers)
                 zuppa_home = BeautifulSoup(risp.text, 'html.parser')
                 volumi = zuppa_home.find_all('a', class_='product-item-link')
                 for v in volumi:
                     tit_vol = v.get_text(strip=True)
                     if tit_vol not in uscite_viste and trovati < 20:
                         uscite_viste.add(tit_vol)
                         jpop_news.append({
                            "signal_text": f"🌸 IN USCITA [J-Pop]: {tit_vol}",
                            "source_key": "jpop",
                            "feed_key": "jpop_releases",
                            "signal_type": "publisher_release"
                        })
                         trovati += 1
    except Exception as e:
        print(f"   ❌ Errore: {e}")

scansiona_jpop()

if jpop_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "jpop_releases").execute()
        supabase.table("external_signal_staging").insert(jpop_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(jpop_news)} segnali J-Pop salvati.")
    except Exception as e:
        pass
