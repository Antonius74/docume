# Knowledge Classifier (FastAPI + Ollama + PostgreSQL)

Prima versione di una piattaforma che:

- riceve upload di file (`pdf`, testo, immagini, audio, video) e link (inclusi YouTube)
- usa Ollama con modello `kimi-k2.5:cloud` per classificazione tematica basata sul contenuto interpretato dall'LLM
- salva file su filesystem e metadati/classificazione su PostgreSQL
- organizza contenuti in cartelle tematiche
- ricerca semantica LLM su titolo, metadati e contenuto estratto (`content_text`)
- UI folder-first: cartelle cliccabili + lista sintetica ultimi upload
- espone API REST + UI responsive (web/mobile) per upload, navigazione e ricerca

## Architettura

- **Backend**: FastAPI
- **LLM locale**: Ollama (`/api/chat`) con output JSON strutturato
- **DB**: PostgreSQL (`resources` table)
- **Storage**:
  - originali in `storage/files/YYYY/MM`
  - riferimenti tematici in `storage/themes/<tema-slug>`

## Avvio rapido

1. Crea virtualenv e installa dipendenze:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configura variabili ambiente:

```bash
cp .env.example .env
```

3. Avvia PostgreSQL usando il tuo stack già esistente (`/Users/antoniolatela/Documents/nexipay_va/docker/docker-compose.postgres.yml`):

```bash
/usr/local/bin/docker compose -f /Users/antoniolatela/Documents/nexipay_va/docker/docker-compose.postgres.yml up -d
```

Il progetto usa di default:
`postgresql+psycopg://postgres:postgres@127.0.0.1:5432/nexi_pay`

`MAX_DOCUMENT_PAGES=10` limita l'estrazione documentale alle prime pagine (PDF/DOC/DOCX) per ingest più veloce.  
Puoi impostarlo a `5` o `10` in base al trade-off velocità/accuratezza.

4. Avvia Ollama e il modello (adatta il nome modello se nel tuo catalogo è diverso):

```bash
ollama pull kimi-k2.5:cloud
ollama run kimi-k2.5:cloud
```

5. Avvia API/UI:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Apri [http://localhost:8000](http://localhost:8000)

## Endpoint REST principali

- `POST /api/ingest/file` upload file multipart (`file`, `title`, `description`)
- `POST /api/ingest/link` upload link JSON (`url`, `title`, `description`)
- `GET /api/resources` ricerca/filtri/sort per pertinenza o data
  - include query semantica (`semantic=true`) con espansione termini (es. `math` -> statistica/geometria)
- `GET /api/resources/recent` ultimi upload sintetici
- `GET /api/resources/{id}` dettaglio elemento
- `GET /api/themes` temi disponibili con conteggio
- `GET /api/folders` preview cartelle tematiche
- `GET /api/files/{id}` download file salvato

## Note prima versione

- Audio/video: in questa versione non c'è trascrizione automatica; la classificazione usa metadati e contesto.
- Se Ollama non è disponibile o il modello non risponde, il sistema usa fallback euristico per non bloccare ingest.
- Per produzione: aggiungere coda async (Celery/RQ), auth, antivirus scanning, rate limits, e embedding search.
