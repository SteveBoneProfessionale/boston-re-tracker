"""Wait for the verification batch, then pull it."""
import json, os, subprocess, sys, time
from anthropic import Anthropic
from dotenv import load_dotenv
load_dotenv()
cl = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
sid = json.load(open("data/ri_verify_batch_state.json"))["id"]
while True:
    b = cl.messages.batches.retrieve(sid)
    print(f"{time.strftime('%H:%M:%S')} {b.processing_status} {b.request_counts}", flush=True)
    if b.processing_status == "ended":
        subprocess.run([sys.executable, "scraper/fetch_batch_results.py",
                        "data/ri_verify_batch_state.json",
                        "data/ri_verify_results.json"], check=False)
        break
    time.sleep(40)
print("done")
