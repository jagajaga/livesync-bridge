#!/usr/bin/env python3
"""
Reconcile the vault filesystem <-> CouchDB, treating the FILESYSTEM as truth.

The bridge's Deno fs watcher (inotify) is best-effort: under bulk file churn the
kernel event queue overflows (IN_Q_OVERFLOW) and there's a watch-add race on
freshly-created directories, so events get DROPPED. That cuts both ways:
  - a deleted folder lingers in CouchDB forever (a "Gmail/" archive once bloated it)
  - a bulk-created folder lands on disk but never uploads (a Gmail backfill stranded
    ~870 files this way)
No watcher fixes this — it's an OS limitation. The fix every serious sync tool uses
is a periodic reconciliation scan, which is this. It runs two ways:
  DELETE: a CouchDB file-doc whose file is gone from disk  -> delete it (tombstone)
  ADD:    a vault file that isn't in CouchDB                -> `touch` it so the
          bridge's watcher fires and uploads it (next bridge restart's walk scan is
          the backstop if a touch is also missed; the next reconcile re-touches too)

SAFETY (this can delete data, so it is paranoid):
  - Case-INSENSITIVE match (CouchDB ids are lowercased; disk is `Calendar/`).
  - Skips chunks (h:), couch internals (_*), device config (ix:*), livesync meta,
    and .obsidian/ (volatile cache — LiveSync handles it).
  - ABORTS if the vault has < MIN_FS_FILES (a missing mount must never read as
    "everything was deleted").
  - DELETE: aborts if > MAX_DELETE_FRAC of file-docs would be deleted (bug guard).
  - ADD: skips (does not touch) if > MAX_TOUCH_FRAC are missing — that means CouchDB
    is mid-rebuild/broken, not that the watcher missed a few; let the rebuild finish.
  - Touches are PACED so they don't re-overflow the same watcher.
  - DRY-RUN by default. Pass --apply to actually delete/touch.
"""
import os, sys, json, time, base64, urllib.request

VAULT = os.environ.get("RECONCILE_VAULT", "/home/jaga/obsidian/data/vault")
ENV = os.environ.get("RECONCILE_ENV", "/home/jaga/obsidian/.env")
MIN_FS_FILES = 1000        # vault has ~10k; far fewer => mount problem => abort
MAX_DELETE_FRAC = 0.10     # never delete >10% of file-docs in one run => abort
MAX_TOUCH_FRAC = 0.50      # >50% missing => couchdb mid-rebuild/broken => don't touch
TOUCH_BATCH = int(os.environ.get("RECONCILE_TOUCH_BATCH", "100"))
TOUCH_SLEEP = float(os.environ.get("RECONCILE_TOUCH_SLEEP", "1.5"))  # pace between batches
APPLY = "--apply" in sys.argv

def load_env(p):
    e = {}
    for line in open(p):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            e[k] = v
    return e

envfile = load_env(ENV) if os.path.exists(ENV) else {}
def cfg(key, default=None):
    return os.environ.get(key, envfile.get(key, default))   # env var wins, then .env, then default
DB = cfg("LIVESYNC_DB", "obsidian")
AUTH = base64.b64encode(f"{cfg('COUCHDB_USER', 'admin')}:{cfg('COUCHDB_PASSWORD')}".encode()).decode()
HOST = os.environ.get("COUCHDB_URL", "http://127.0.0.1:5984")

def req(method, path, data=None):
    r = urllib.request.Request(HOST + path, data=data, method=method,
        headers={"Authorization": "Basic " + AUTH, "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(r))

def is_file_doc(i):
    return not (i.startswith("h:") or i.startswith("_") or i.startswith("ix:")
                or i.startswith("obsydian") or i.startswith("obsidian_livesync")
                or i.startswith(".obsidian/"))

# --- 1. walk the vault once: fs_all (all, for delete side) + fs_sync (syncable, for add side)
fs_all = set()                 # lowercased rel paths of every file
fs_sync = {}                   # lowercased rel path -> real abs path, syncable files only
for root, _dirs, files in os.walk(VAULT):
    for f in files:
        rel = os.path.relpath(os.path.join(root, f), VAULT).replace(os.sep, "/")
        lc = rel.lower()
        fs_all.add(lc)
        if is_file_doc(lc):
            fs_sync[lc] = os.path.join(root, f)
if len(fs_all) < MIN_FS_FILES:
    sys.exit(f"ABORT: vault has only {len(fs_all)} files (< {MIN_FS_FILES}) — mount problem? Doing nothing.")

# --- 2. CouchDB file-docs (ranges that bracket the h: chunk block)
filedocs = []
couch_lc = set()
for params in ('startkey=%22%22&endkey=%22h%3A%22', 'startkey=%22h%3B%22'):
    for r in req("GET", f"/{DB}/_all_docs?{params}")["rows"]:
        if is_file_doc(r["id"]):
            filedocs.append((r["id"], r["value"]["rev"]))
            couch_lc.add(r["id"].lower())

orphans = [(i, rev) for (i, rev) in filedocs if i.lower() not in fs_all]     # in couch, gone from disk
missing = [real for lc, real in fs_sync.items() if lc not in couch_lc]       # on disk, not in couch
total = len(filedocs)
print(f"vault files={len(fs_all)} couchdb file-docs={total}  orphans(delete)={len(orphans)} "
      f"missing(add)={len(missing)}  mode={'APPLY' if APPLY else 'dry-run'}")

# --- 3. DELETE side (strict guard) ---
if orphans:
    if total and len(orphans) / total > MAX_DELETE_FRAC:
        for i, _ in orphans[:20]:
            print("  would delete:", i, file=sys.stderr)
        print(f"SKIP DELETE: {len(orphans)}/{total} ({100*len(orphans)//total}%) exceeds "
              f"{int(MAX_DELETE_FRAC*100)}% limit — investigate (case mismatch? wrong vault?).", file=sys.stderr)
    else:
        for i, _ in orphans[:10]:
            print("  orphan:", i)
        if APPLY:
            dels = [{"_id": i, "_rev": rev, "_deleted": True} for (i, rev) in orphans]
            done = 0
            for j in range(0, len(dels), 2000):
                res = req("POST", f"/{DB}/_bulk_docs", json.dumps({"docs": dels[j:j+2000]}).encode())
                done += sum(1 for x in res if x.get("ok"))
            print(f"deleted {done} orphaned file-docs.")
        else:
            print(f"DRY RUN — would delete {len(orphans)} orphaned file-docs.")

# --- 4. ADD side (touch missing so the bridge uploads them; non-destructive, paced) ---
if missing:
    if len(missing) / max(1, len(fs_sync)) > MAX_TOUCH_FRAC:
        print(f"SKIP ADD: {len(missing)}/{len(fs_sync)} files missing — CouchDB looks mid-rebuild/"
              f"broken, not a watcher miss. Not touching.", file=sys.stderr)
    else:
        for real in missing[:10]:
            print("  missing:", os.path.relpath(real, VAULT))
        if APPLY:
            touched = 0
            for k in range(0, len(missing), TOUCH_BATCH):
                for real in missing[k:k+TOUCH_BATCH]:
                    try:
                        os.utime(real, None)   # mtime=now -> watcher 'modify' event -> bridge uploads
                        touched += 1
                    except OSError:
                        pass
                if k + TOUCH_BATCH < len(missing):
                    time.sleep(TOUCH_SLEEP)    # pace so we don't re-overflow the watcher
            print(f"touched {touched} missing files (bridge will upload them).")
        else:
            print(f"DRY RUN — would touch {len(missing)} missing files.")
