# Synthea Data Ingestion Tool for Medplum

A fully decoupled, standalone, high-performance FHIR ingestion dashboard explicitly engineered to intelligently parse, chunk, and stream massive Synthea payload arrays (`.zip` or `.json`) seamlessly into Medplum EMR without causing Postgres database deadlocks or API rate limit crashes.

![Dashboard Preview](https://img.shields.io/badge/Production-Ready-brightgreen)
![FastAPI Backend](https://img.shields.io/badge/Backend-FastAPI-blue)
![Architecture](https://img.shields.io/badge/Infrastucture-WebSocket%20Streaming-orange)

## 📌 Core Features

1. **Automated FHIR Validation & Payload Integrity**: Automatically maps monolithic Synthea records, corrects `urn:uuid:` relative links to Medplum-compatible Conditional Upserts (`?identifier=...`), and maintains hard referential integrity natively.
2. **Infinite Scaling & Deadlock Prevention**: The background worker explicitly slices huge bulk FHIR files into optimized bitesize transaction chunks (max 20 atomic entries) on the fly, explicitly circumventing Postgres deadlocks and `429` Rate Limit errors on the Medplum DB!
3. **Decoupled Security**: Explicitly loads OAuth2 tokens through local `.env` mappings completely avoiding hardcoded pipeline credentials.
4. **WebSocket Terminal Tracking**: Boasts a fully reactive, beautifully-styled Glassmorphic UI that bridges raw Python `stdout` diagnostics directly to your local Web-Browser via an unblocked, threaded OS architecture pipeline.

---

## 🚀 Quickstart Installation

Ensure you have Python 3.9+ actively installed so you can run multi-threaded sub-workers.

1. **Clone the Tool:**
   ```bash
   git clone https://github.com/YourUsername/synthea-medplum-ingestion.git
   cd synthea-medplum-ingestion
   ```
2. **Install Fast Requirements:**
   ```bash
   pip install fastapi uvicorn requests python-dotenv python-multipart
   ```
3. **Configure Pipeline Secrets:**
   Create a pure `.env` file directly squarely in the root dictionary:
   ```env
   MEDPLUM_CLIENT_ID="your_super_secret_id"
   MEDPLUM_CLIENT_SECRET="your_super_secret_key"
   MEDPLUM_BASE_URL="http://localhost:8103/fhir/R4"
   MEDPLUM_TOKEN_URL="http://localhost:8103/oauth2/token"
   ```
4. **Boot the Glass Dashboard:**
   ```bash
   uvicorn main:app --reload
   ```

Launch `http://localhost:8000` physically inside your browser. Drag and drop your massive Synthea output `split.zip` database block right onto the panel, and vividly monitor the backend data orchestration engine streaming real-time directly on the terminal!

---
## 🏗️ Local Development

- `main.py` -> The central ASGI router that bridges user requests logically and physically isolates standard OS threads.
- `ingest.py` -> The standalone data orchestration worker that organically parses, splits, validates, and uploads chunks recursively.
- `index.html` -> The fully-styled real-time WebSocket front-end architecture!
