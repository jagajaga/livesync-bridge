#!/usr/bin/env python3
"""
Reconcile CouchDB -> vault filesystem deletions.

The bridge's Deno fs watcher catches LIVE deletions, but misses them when they
happen while it's down/restarting (or in bulk) — leaving orphaned file-docs in
CouchDB (e.g. a deleted "Gmail/" archive that kept bloating the DB). This script
deletes any CouchDB *file-doc* whose file no longer exists on the vault
filesystem, so the filesystem stays the source of truth.

SAFETY (this script can delete data, so it is paranoid):
  - Matches case-INSENSITIVELY (CouchDB ids are lowercased; disk is `Calendar/`).
  - Skips chunks (h:), couch internals (_*), device config (ix:*), livesync meta,
    and .obsidian/ (volatile cache — handled by LiveSync itself).
  - ABORTS if the vault has fewer than MIN_FS_FILES (a missing mount must never
    be read as "everything was deleted").
  - ABORTS if more than MAX_DELETE_FRAC of file-docs would be deleted (a bug or a
    case-normalisation mismatch must not wipe the DB).
  - DRY-RUN by default. Pass --apply to actually delete.
"""
import os, sys, json, base64, urllib.request

# Env-driven so the SAME script runs on the host (cron, reads ./ .env, talks to
# 127.0.0.1:5984, vault under data/) and INSIDE the bridge container (entrypoint
# loop, env already set, vault at /vault, couchdb at couchdb:5984).
VAULT = os.environ.get("RECONCILE_VAULT", "/home/jaga/obsidian/data/vault")
ENV = os.environ.get("RECONCILE_ENV", "/home/jaga/obsidian/.env")
MIN_FS_FILES = 1000        # vault has ~8.7k; far fewer => mount problem => abort
MAX_DELETE_FRAC = 0.10     # never delete >10% of file-docs in one run => abort
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
USER = cfg("COUCHDB_USER", "admin")
PW = cfg("COUCHDB_PASSWORD")
AUTH = base64.b64encode(f"{USER}:{PW}".encode()).decode()
HOST = os.environ.get("COUCHDB_URL", "http://127.0.0.1:5984")

def req(method, path, data=None):
    r = urllib.request.Request(HOST + path, data=data, method=method,
        headers={"Authorization": "Basic " + AUTH, "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(r))

def is_file_doc(i):
    return not (i.startswith("h:") or i.startswith("_") or i.startswith("ix:")
                or i.startswith("obsydian") or i.startswith("obsidian_livesync")
                or i.startswith(".obsidian/"))

# 1. lowercased filesystem path set
fs = set()
for root, _dirs, files in os.walk(VAULT):
    for f in files:
        rel = os.path.relpath(os.path.join(root, f), VAULT).replace(os.sep, "/")
        fs.add(rel.lower())
if len(fs) < MIN_FS_FILES:
    sys.exit(f"ABORT: vault has only {len(fs)} files (< {MIN_FS_FILES}) — mount problem? Deleting nothing.")

# 2. CouchDB file-docs (ranges that bracket the h: chunk block)
filedocs = []
for params in ('startkey=%22%22&endkey=%22h%3A%22', 'startkey=%22h%3B%22'):
    for r in req("GET", f"/{DB}/_all_docs?{params}")["rows"]:
        if is_file_doc(r["id"]):
            filedocs.append((r["id"], r["value"]["rev"]))

# 3. orphans (case-insensitive)
orphans = [(i, rev) for (i, rev) in filedocs if i.lower() not in fs]
total = len(filedocs)
print(f"vault files={len(fs)}  couchdb file-docs={total}  orphans={len(orphans)}  mode={'APPLY' if APPLY else 'dry-run'}")
if not orphans:
    sys.exit(0)

# 4. mass-delete guard
if total and len(orphans) / total > MAX_DELETE_FRAC:
    for i, _ in orphans[:20]:
        print("  would delete:", i, file=sys.stderr)
    sys.exit(f"ABORT: {len(orphans)}/{total} ({100*len(orphans)//total}%) would be deleted — exceeds "
             f"{int(MAX_DELETE_FRAC*100)}% limit. Investigate (case mismatch? wrong vault?). Deleting nothing.")

for i, _ in orphans[:15]:
    print("  orphan:", i)
if not APPLY:
    print(f"DRY RUN — would delete {len(orphans)} orphaned file-docs. Re-run with --apply.")
    sys.exit(0)

# 5. delete (tombstone) in batches
dels = [{"_id": i, "_rev": rev, "_deleted": True} for (i, rev) in orphans]
done = 0
for j in range(0, len(dels), 2000):
    res = req("POST", f"/{DB}/_bulk_docs", json.dumps({"docs": dels[j:j+2000]}).encode())
    done += sum(1 for x in res if x.get("ok"))
print(f"deleted {done} orphaned file-docs.")
