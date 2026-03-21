# Feed classifiche automatico

## Cosa fa
Questo pacchetto genera e aggiorna automaticamente `classifiche.json`, il file che la tua app legge per dare suggerimenti smart sui libri in classifica.

## File inclusi
- `classifiche.json` → seed iniziale
- `scripts/update_classifiche.py` → scraper/generatore del feed
- `.github/workflows/update-classifiche.yml` → aggiornamento automatico con GitHub Actions
- `requirements-classifiche.txt` → dipendenze Python

## Dove metterli
Copia questi file nella root del repository che ospita la tua app, mantenendo la cartella `.github/workflows` e `scripts`.

Struttura finale consigliata:

```text
/index.html
/classifiche.json
/requirements-classifiche.txt
/scripts/update_classifiche.py
/.github/workflows/update-classifiche.yml
```

## Come si aggiorna automaticamente
1. Pubblica questi file nel repo GitHub della tua app.
2. Vai su **Settings → Actions → General** e lascia attive le GitHub Actions.
3. Vai su **Actions** e lancia una volta manualmente `Update classifiche`.
4. Da quel momento il workflow aggiornerà `classifiche.json` due volte al giorno e farà commit automatico solo se il file cambia.

## Come lo legge la app
La build smart delle classifiche cerca per default `./classifiche.json`, quindi se il file sta nella stessa cartella dell'HTML non devi cambiare URL.

Se vuoi usare un URL diverso, nella console del browser puoi impostarlo così:

```js
setClassificheFeedUrl('https://tuodominio.it/classifiche.json')
```

## Nota onesta
Il seed incluso è solo un bootstrap. Il vero feed “completo” viene costruito dallo script alla prima esecuzione del workflow. Se IBS cambia HTML o struttura delle pagine, lo scraper potrebbe aver bisogno di un piccolo adattamento.
