!pip install -q supabase requests beautifulsoup4 lxml

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- LE CHIAVI DEL TUO CERVELLONE ---
SUPABASE_URL = "https://aacqebirvnkrbewvgmvo.supabase.co"
SUPABASE_KEY = "sb_publishable_opJ7oXwCxaT53ym88mSxOA_-iNxA1f3"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

tiktok_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("📱 RADAR BOOKTOK ATTIVATO: Ricerca dei trend virali...\n")

def scansiona_trend_tiktok():
    print("📍 Intercettazione frequenze BookTok/MangaTok via Google Radar...")
    # Cerchiamo notizie italiane degli ultimi 7 giorni (when:7d) sui trend BookTok
    url = "https://news.google.com/rss/search?q=BookTok+OR+MangaTok+libri+OR+fumetti+when:7d&hl=it&gl=IT&ceid=IT:it"
    
    try:
        risposta = requests.get(url, headers=headers, timeout=10)
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.content, 'xml')
            entries = zuppa.find_all('item')
            trovati = 0
            
            for entry in entries:
                titolo = entry.title.text.strip()
                
                # Puliamo il titolo (Google News aggiunge spesso " - Nome Testata" alla fine)
                titolo_pulito = titolo.split(" - ")[0]
                
                if len(titolo_pulito) > 15 and trovati < 8:
                    tiktok_news.append({
                        "signal_text": f"📱 TREND TIKTOK: {titolo_pulito}",
                        "source_key": "google-trends",
                        "feed_key": "booktok_trends",
                        "signal_type": "national_trend"
                    })
                    print(f"   🎵 {titolo_pulito[:70]}...")
                    trovati += 1
        else:
            print(f"   ❌ Bloccato (Codice: {risposta.status_code})")
    except Exception as e:
        print(f"   ❌ Errore di decrittazione: {e}")
    print("-" * 40)

scansiona_trend_tiktok()

print("\n🧠 Iniezione nel database Supabase...")

if tiktok_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "booktok_trends").execute()
        supabase.table("external_signal_staging").insert(tiktok_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(tiktok_news)} trend di TikTok iniettati nel Cervellone.")
    except Exception as e:
        print(f"❌ Errore di scrittura: {e}")
else:
    print("Nessun trend rilevato. I ragazzini sono a scuola.")
