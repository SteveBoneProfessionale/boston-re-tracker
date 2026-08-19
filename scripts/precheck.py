"""Pre-push check: every source file must compile and the app must render.

WHY THIS EXISTS. A malformed f-string in app/main.py reached the deployed
site. The AppTest smoke test I had been relying on reported a clean run for
it, because AppTest.from_file swallows an import-time SyntaxError in the
script under test and returns an empty exception list -- a clean result and a
broken file look identical through it.

compileall catches exactly that class and costs nothing, so it runs first.
"""
import subprocess
import sys

ok = True

r = subprocess.run([sys.executable, "-m", "compileall", "-q", "app", "scraper", "db"],
                   capture_output=True, text=True)
if r.returncode != 0:
    ok = False
    print("COMPILE FAILED")
    print(r.stdout or "", r.stderr or "")
else:
    print("compile: OK")

try:
    from streamlit.testing.v1 import AppTest
    a = AppTest.from_file("app/main.py", default_timeout=200).run()
    if len(a.exception):
        ok = False
        print("APP RAISED:", a.exception)
    else:
        print("app renders: OK")
except Exception as e:                                          # noqa: BLE001
    ok = False
    print("APP FAILED TO START:", type(e).__name__, e)

print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
