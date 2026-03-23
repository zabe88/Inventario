import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- LE CHIAVI DEL TUO CERVELLONE ---
SUPABASE_URL = "https://aacqebirvnkrbewvgmvo.supabase.co"
SUPABASE_KEY = "sb_publishable_opJ7oXwCxaT53ym88mSxOA_-iNxA1f3"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

mondadori_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("🦅 ORACOLO MONDADORI V2: Assalto ai domini gemelli...\n")

def scansiona_mondadori_doppio():
    uscite_viste = set()
    trovati = 0
    
    # BERSAGLIO 1: Il sito principale (Varia, Romanzi, Saggi)
    print("📍 Infiltrazione in Mondadori Generale...")
    url_main = "https://www.mondadori.it/libri-prossime-uscite/"
    try:
        risp = requests.get(url_main, headers=headers, timeout=15)
        if risp.status_code == 200:
            zuppa = BeautifulSoup(risp.text, 'html.parser')
            # Mondadori usa liste e link interni ai post
            titoli_tag = zuppa.find_all(['h2', 'h3', 'a'])
            for t in titoli_tag:
                titolo = t.get_text(strip=True)
                if len(titolo) > 10 and "Scopri" not in titolo and "Leggi tutto" not in titolo and "Mondadori" not in titolo:
                    if titolo not in uscite_viste and trovati < 15:
                        uscite_viste.add(titolo)
                        mondadori_news.append({
                            "signal_text": f"🦅 IN USCITA [Mondadori]: {titolo}",
                            "source_key": "mondadori",
                            "feed_key": "mondadori_releases",
                            "signal_type": "publisher_release"
                        })
                        print(f"   📘 {titolo[:60]}...")
                        trovati += 1
        else:
            print(f"   ❌ Muro alzato su Mondadori (Codice: {risp.status_code})")
    except Exception as e:
        print(f"   ❌ Errore: {e}")

    # BERSAGLIO 2: Il Caveau Nerd (Oscar Vault, Fantasy, Sci-Fi)
    print("\n📍 Infiltrazione nel Caveau Oscar Mondadori...")
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
                        mondadori_news.append({
                            "signal_text": f"🗡️ IN USCITA [Oscar Vault]: {titolo}",
                            "source_key": "mondadori",
                            "feed_key": "mondadori_releases",
                            "signal_type": "publisher_release"
                        })
                        print(f"   🗡️ {titolo[:60]}...")
                        trovati += 1
        else:
             print(f"   ❌ Muro alzato su Oscar Mondadori (Codice: {risp_oscar.status_code})")
    except Exception as e:
        print(f"   ❌ Errore: {e}")
        
    print("-" * 40)

scansiona_mondadori_doppio()

print("\n🧠 Iniezione nel database Supabase...")

if mondadori_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "mondadori_releases").execute()
        supabase.table("external_signal_staging").insert(mondadori_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(mondadori_news)} novità Mondadori/Vault immagazzinate.")
    except Exception as e:
        print(f"❌ Errore di scrittura: {e}")
else:
    print("Nessun dato. L'impero si è difeso.")
