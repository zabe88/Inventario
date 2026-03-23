import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- LE CHIAVI DEL TUO CERVELLONE ---
SUPABASE_URL = "https://aacqebirvnkrbewvgmvo.supabase.co"
SUPABASE_KEY = "sb_publishable_opJ7oXwCxaT53ym88mSxOA_-iNxA1f3"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

anime_news = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, come Gecko) Chrome/91.0.4472.124 Safari/537.36'}

print("🌋 SISMOGRAFO ANIME ATTIVATO: Ricerca di nuovi adattamenti e trailer...\n")

def scansiona_animeclick():
    print("📍 Infiltrazione nei link profondi di AnimeClick...")
    try:
        url = "https://www.animeclick.it/"
        risposta = requests.get(url, headers=headers, timeout=10)
        
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            tutti_i_link = zuppa.find_all('a')
            titoli_visti = set()
            trovati = 0
            
            for el in tutti_i_link:
                testo = el.get_text(strip=True)
                link_url = el.get('href', '')
                
                if len(testo) > 25 and ('/news/' in link_url or '/notizi' in link_url or len(link_url) > 15):
                    if testo not in titoli_visti and trovati < 20:
                        titoli_visti.add(testo)
                        
                        hype_words = ['annunciato', 'stagione', 'trailer', 'adattamento', 'anime', 'film']
                        is_hype = any(parola in testo.lower() for parola in hype_words)
                        
                        prefisso = "🔥 HYPE ANIME:" if is_hype else "📺 NEWS ANIME:"
                        
                        anime_news.append({
                            "signal_text": f"{prefisso} {testo}",
                            "source_key": "animeclick",
                            "feed_key": "animeclick_news",
                            "signal_type": "national_trend"
                        })
                        trovati += 1
    except Exception as e:
        print(f"   ❌ Errore: {e}")

scansiona_animeclick()

print("\n🧠 Iniezione nel database Supabase...")

if anime_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "animeclick_news").execute()
        supabase.table("external_signal_staging").insert(anime_news).execute()
        print(f"✅ VITTORIA TOTALE! Inserite {len(anime_news)} notizie anime nel database.")
    except Exception as e:
        print(f"❌ Errore di scrittura: {e}")
