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
reddit_news = []
# Reddit blocca i bot normali. Ci travestiamo da "Cervellone" autorizzato.
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CervelloneBot/1.0'}

print("🗣️ RADAR PASSAPAROLA ATTIVATO: Intercettazione discussioni sui forum...\n")

def scansiona_reddit(subreddit):
    print(f"📍 Origliando nella piazza di r/{subreddit}...")
    # Usiamo il trucco dell'estensione .rss per superare i blocchi
    url = f"https://www.reddit.com/r/{subreddit}/hot/.rss?limit=12"
    try:
        risposta = requests.get(url, headers=headers, timeout=10)
        
        if risposta.status_code == 200:
            # BeautifulSoup legge il formato XML/RSS
            zuppa = BeautifulSoup(risposta.content, 'xml')
            entries = zuppa.find_all('entry')
            trovati = 0
            
            for entry in entries:
                titolo = entry.title.text.strip()
                
                # Ignoriamo i post di servizio fissati in alto dai moderatori
                if "Megathread" in titolo or "Regolamento" in titolo or "Consigli per gli acquisti" in titolo:
                    continue
                
                if len(titolo) > 15 and trovati < 8:
                    reddit_news.append({
                        "signal_text": f"🗣️ TREND [r/{subreddit}]: {titolo}",
                        "source_key": "reddit",
                        "feed_key": "reddit_trends",
                        "signal_type": "national_trend"
                    })
                    print(f"   💬 {titolo[:70]}...")
                    trovati += 1
        else:
            print(f"   ❌ Muro alzato (Codice: {risposta.status_code})")
    except Exception as e:
        print(f"   ❌ Errore di decrittazione: {e}")
    print("-" * 40)

# Ascoltiamo i 3 poli principali dell'Hype Nerd e Letterario
scansiona_reddit("Libri")
scansiona_reddit("AnimeItaly")
scansiona_reddit("fumetti")

print("\n🧠 Iniezione nel database Supabase...")

if reddit_news:
    try:
        supabase.table("external_signal_staging").delete().eq("feed_key", "reddit_trends").execute()
        supabase.table("external_signal_staging").insert(reddit_news).execute()
        print(f"✅ VITTORIA TOTALE! {len(reddit_news)} discussioni virali iniettate nel Cervellone.")
    except Exception as e:
        print(f"❌ Errore di scrittura: {e}")
else:
    print("Nessun dato estratto. I forum sono silenziosi oggi.")
