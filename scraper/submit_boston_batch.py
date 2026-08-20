"""Submit the Article 80 extraction to the Batch API and record the batch id."""
import json
import os
import sys
from pathlib import Path

from anthropic import Anthropic
from anthropic.types.messages.batch_create_params import Request
from dotenv import load_dotenv

load_dotenv()
REQ = Path("data/boston_batch_requests.jsonl")
STATE = Path("data/boston_batch_state.json")


def main():
    reqs = [json.loads(l) for l in REQ.read_text(encoding="utf-8").splitlines() if l.strip()]
    print(f"{len(reqs)} requests")
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    batch = client.messages.batches.create(
        requests=[Request(custom_id=r["custom_id"], params=r["params"]) for r in reqs]
    )
    print("batch id:", batch.id)
    print("status  :", batch.processing_status)
    STATE.write_text(json.dumps({
        "id": batch.id,
        "status": batch.processing_status,
        "n_requests": len(reqs),
        "created_at": str(batch.created_at),
    }, indent=1))
    print(f"wrote {STATE}")


if __name__ == "__main__":
    main()
