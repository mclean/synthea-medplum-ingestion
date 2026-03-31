import asyncio
from fastapi import FastAPI, UploadFile, File, WebSocket, BackgroundTasks, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import shutil
import os
import uvicorn
import logging
import subprocess
import threading

# Ensure logs are persistent dynamically
logging.basicConfig(filename="ingestion_audit.log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI()

os.makedirs("temp_uploads", exist_ok=True)

connected_websockets = set()
MAIN_LOOP = None

@app.on_event("startup")
async def startup_event():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

async def broadcast_log(message: str):
    logging.info(message)
    print(message)
    connections = list(connected_websockets)
    for ws in connections:
        try:
            await ws.send_text(message)
        except Exception:
            connected_websockets.remove(ws)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)

def run_ingest_worker(script_path: str, filepath: str):
    # This runs securely on a dedicated background Python thread
    # Combines stdout and stderr so we just read one stream
    process = subprocess.Popen(
        ["python", script_path, filepath],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=os.path.dirname(script_path),
        encoding="utf-8",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"}
    )
    
    for line in iter(process.stdout.readline, ''):
        msg = line.strip()
        if msg:
            try:
                # Schedule the WebSocket broadcast physically onto the main async event loop
                asyncio.run_coroutine_threadsafe(broadcast_log(f"⚡ {msg}"), MAIN_LOOP)
            except Exception:
                pass

    process.stdout.close()
    rc = process.wait()
    
    if rc == 0:
        asyncio.run_coroutine_threadsafe(broadcast_log("🎉 ACRO Production Pipeline Sub-worker cleanly terminated!"), MAIN_LOOP)
    else:
        asyncio.run_coroutine_threadsafe(broadcast_log(f"❌ Fatal Sub-worker Crash! Process aborted with exit status code {rc}. Please inspect ingestion_audit.log"), MAIN_LOOP)

async def execute_acro_ingest(filepath: str):
    await broadcast_log(f"✅ Secure ACRO handshake confirmed: Receiving payload natively...")
    await broadcast_log(f"📦 Architecting dedicated unblocking sub-worker thread for isolated orchestration...")
    
    script_path = os.path.join(os.path.dirname(__file__), "ingest.py")
    
    # Fully bypass Windows SelectorEventLoop bugs by manually orchestrating true threads
    worker_thread = threading.Thread(target=run_ingest_worker, args=(script_path, filepath))
    worker_thread.start()

@app.get("/")
async def get_index():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read(), status_code=200)

@app.post("/api/ingest")
async def ingest_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    file_location = os.path.join("temp_uploads", file.filename)
    with open(file_location, "wb+") as file_object:
        shutil.copyfileobj(file.file, file_object)
    
    background_tasks.add_task(execute_acro_ingest, file_location)
    
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
