"""Wait for the named batches to finish, then pull their results to disk."""
import json
import subprocess
import sys
import time
from pathlib import Path

import os
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

JOBS = [
    ("data/ri_minutes_batch_state.json", "data/ri_minutes_results.json"),
    ("data/ri_scan_batch_state.json", "data/ri_scan_results.json"),
]


def main():
    cl = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    pending = list(JOBS)
    while pending:
        still = []
        for state, out in pending:
            sid = json.loads(Path(state).read_text())["id"]
            b = cl.messages.batches.retrieve(sid)
            print(f"{time.strftime('%H:%M:%S')} {sid[-8:]} {b.processing_status} "
                  f"{b.request_counts}", flush=True)
            if b.processing_status == "ended":
                subprocess.run([sys.executable, "scraper/fetch_batch_results.py",
                                state, out], check=False)
            else:
                still.append((state, out))
        pending = still
        if pending:
            time.sleep(45)
    print("all batches ended")


if __name__ == "__main__":
    main()
