import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import re

# --- LE CHIAVI DEL TUO CERVELLONE ---
SUPABASE_URL = "https://aacqebirvnkrbewvgmvo.supabase.co"
SUPABASE_KEY = "sb_publishable_opJ7oXwCxaT53ym88mSxOA_-iNxA1f3"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

segnali_raccolti = []
calendario_eventi = [] 
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, come Gecko) Chrome/91.0.4472.124 Safari/537.36'}

print("🕷️ SCIAME CRONOLOGICO ATTIVATO: Ricerca eventi futuri in Lunigiana...\n")

regex_date = re.compile(r'\b(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\b', re.IGNORECASE)

def scansiona_sito(nome_fonte, url, source_key, feed_key, tags=['h2', 'h3', 'h4', 'p']):
    print(f"📍 Analisi {nome_fonte} ...")
    try:
        risposta = requests.get(url, headers=headers, timeout=10)
        if risposta.status_code == 200:
            zuppa = BeautifulSoup(risposta.text, 'html.parser')
            elementi = zuppa.find_all(tags)
            trovati = 0
            
            for el in elementi:
                testo = el.get_text(strip=True)
                if len(testo) > 20 and trovati < 8 and "Leggi tutto" not in testo:
                    
                    data_trovata = regex_date.search(testo)
                    
                    if data_trovata:
                        mese_giorno = data_trovata.group(0).upper()
                        calendario_eventi.append({
                            "signal_text": f"🗓️ EVENTO [{mese_giorno}]: {testo} ({nome_fonte})",
                            "source_key": source_key, 
                            "feed_key": feed_key,
                            "signal_type": "local_event"
                        })
                        trovati += 1
                    else:
                        if el.name in ['h2', 'h3']: 
                            segnali_raccolti.append({
                                "signal_text": f"{nome_fonte}: {testo}",
                                "source_key": source_key, 
                                "feed_key": feed_key,
                                "signal_type": "local_news"
                            })
                            trovati += 1
    except Exception as e:
        print(f"   ❌ Errore su {nome_fonte}: {e}")

# LANCIAMO I RAGNI 
scansiona_sito("ECO LUNIGIANA", "https://www.ecodellalunigiana.it/category/pontremoli/", "visitlunigiana", "eco_lunigiana")
scansiona_sito("SIGERIC", "https://www.sigeric.it/", "sigeric", "sigeric_web")
scansiona_sito("LUNIGIANA WORLD", "https://lunigianaworld.com/", "lunigianaworld", "lunigiana_world")
scansiona_sito("VISIT PONTREMOLI", "https://visitpontremoli.it/", "visitlunigiana", "visit_pontremoli")
scansiona_sito("FARFALLE IN CAMMINO", "https://www.farfalleincammino.org/", "visitlunigiana", "farfalle_cammino")

tutti_i_dati = segnali_raccolti + calendario_eventi

if tutti_i_dati:
    try:
        feed_da_cancellare = ["eco_lunigiana", "sigeric_web", "lunigiana_world", "visit_pontremoli", "farfalle_cammino"]
        supabase.table("external_signal_staging").delete().in_("feed_key", feed_da_cancellare).execute()
        supabase.table("external_signal_staging").insert(tutti_i_dati).execute()
        print(f"✅ VITTORIA TOTALE! {len(segnali_raccolti)} notizie e {len(calendario_eventi)} EVENTI iniettati.")
    except Exception as e:
        print(f"❌ Errore di scrittura: {e}")
