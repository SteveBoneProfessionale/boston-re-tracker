"""Pull a finished batch's results to disk and report actual token spend."""
import json
import os
import sys
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

# Haiku 4.5 list price; batch is billed at half.
IN_RATE, OUT_RATE = 1.00, 5.00


def main(state_file, out_file):
    sid = json.loads(Path(state_file).read_text())["id"]
    cl = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    b = cl.messages.batches.retrieve(sid)
    print(f"{sid}: {b.processing_status} {b.request_counts}")
    if b.processing_status != "ended":
        return 1
    rows, tin, tout, errs = [], 0, 0, 0
    for r in cl.messages.batches.results(sid):
        d = {"custom_id": r.custom_id, "type": r.result.type}
        if r.result.type == "succeeded":
            m = r.result.message
            d["text"] = "".join(c.text for c in m.content if c.type == "text")
            tin += m.usage.input_tokens
            tout += m.usage.output_tokens
        else:
            errs += 1
            d["error"] = str(getattr(r.result, "error", r.result.type))
        rows.append(d)
    Path(out_file).write_text(json.dumps(rows, indent=1))
    cost = (tin / 1e6 * IN_RATE + tout / 1e6 * OUT_RATE) / 2
    print(f"results {len(rows)}  errors {errs}")
    print(f"tokens in {tin:,}  out {tout:,}")
    print(f"ACTUAL BATCH COST: ${cost:.2f}")
    Path(out_file + ".cost").write_text(json.dumps(
        {"input": tin, "output": tout, "cost_usd": round(cost, 4)}, indent=1))
    print("wrote", out_file)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
