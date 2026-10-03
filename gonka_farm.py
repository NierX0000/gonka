#!/usr/bin/env python3
"""
gonka_farm.py — bulk register + ambil API key GonkaAPI (gonka-api.org).

Fakta yang SUDAH diverifikasi langsung (bukan tebakan)
-----------------------------------------------------
  arsitektur : SPA Vite + Supabase (managed auth)
  URL Supabase: https://hskyauefqcgbvgvxkluj.supabase.co
  validasi   : POST /functions/v1/validate-email
               -> {ok:false, reason:"disposable_domain"} untuk domain temp terkenal
               -> LOLOS untuk maxxspace.com (mail.tm), grabmail.io, emcognito.com
  signup     : POST /auth/v1/signup  {email, password, data:{ref_code}}
               -> wajib konfirmasi email via LINK (bukan OTP)
  verifikasi : buka link https://.../auth/v1/verify?token=...&type=signup&redirect_to=...
  login      : POST /auth/v1/token?grant_type=password {email, password} -> JWT
  API key    : INSERT api_keys {user_id}  (pakai Supabase REST, auth Bearer JWT)
  referral   : direkam via user_metadata.ref_code (dari ?friend= di UI)

Inbox temp: GrabMail (grabmail.io) — domain LOLOS blacklist, gratis, API tan pakai
  key/akun, BISA baca isi email (untuk ambil link konfirmasi).
  read: GET https://grabmail.io/api/v1/mailbox?address=<addr>

Pakai
-----
  python gonka_farm.py 5            # 5 akun
  python gonka_farm.py 5 -w 3       # 5 akun, 3 worker paralel
  python gonka_farm.py 5 --sleep 10 # jeda antar akun 10s
  python gonka_farm.py 5 --proxy "user:pass@host:port"   # 1 proxy (BrightData, dll)
  python gonka_farm.py 5 --proxy "127.0.0.1:8080"        # proxy lokal tanpa auth
  python gonka_farm.py 5 --proxy-file proxies.txt        # banyak proxy round-robin
  python gonka_farm.py --check      # cek config + akses Supabase

contoh proxies.txt (1 per baris):
  user:pass@host1:port
  127.0.0.1:8080
  socks5://user:pass@host:port
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import string
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import ProxyHandler, build_opener

FILE_LOCK = threading.Lock()

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_KEYS = os.path.join(HERE, "keys.txt")
OUT_LOG = os.path.join(HERE, "accounts.jsonl")

SUPABASE = "https://hskyauefqcgbvgvxkluj.supabase.co"
AUTH_URL = f"{SUPABASE}/auth/v1"
REST_URL = f"{SUPABASE}/rest/v1"
FUNCTIONS_URL = f"{SUPABASE}/functions/v1"

# anon key (publik, sudah ada di bundle frontend)
_ANON_KEY = None
def anon_key():
    global _ANON_KEY
    if _ANON_KEY:
        return _ANON_KEY
    b = os.path.join(HERE, "anon_key.txt")
    if os.path.exists(b):
        _ANON_KEY = open(b).read().strip()
        return _ANON_KEY
    # coba ekstrak dari bundle lokal bila tersedia
    for cand in ("/tmp/gonka_bundle.js",):
        try:
            src = open(cand).read()
            m = re.search(r'eyJ[A-Za-z0-9_.-]{60,}', src)
            if m:
                _ANON_KEY = m.group(0)
                return _ANON_KEY
        except Exception:
            pass
    raise RuntimeError("Tidak dapat anon key. Letakkan string key di anon_key.txt")

REF_CODE = os.getenv("GONKA_REF", "WJ2JNSFA")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"

# ────────────── proxy (thread-local, rotasi per akun) ──────────────
_PROXY_LIST = []          # daftar proxy: "user:pass@host:port" atau "host:port"
_PROXY_STATE = threading.local()  # tiap thread punya proxy sendiri
_PROXY_RR = 0             # counter round-robin global (untuk mode sekuensial)
_PROXY_RR_LOCK = threading.Lock()

def _proxy_opener(proxy):
    """Buat opener dengan proxy; None = direct."""
    if not proxy:
        return build_opener()
    purl = proxy if "://" in proxy else "http://" + proxy
    return build_opener(ProxyHandler({"http": purl, "https": purl}))

def _next_proxy():
    if not _PROXY_LIST:
        return None
    global _PROXY_RR
    with _PROXY_RR_LOCK:
        p = _PROXY_LIST[_PROXY_RR % len(_PROXY_LIST)]
        _PROXY_RR += 1
    return p

def proxy_for_thread():
    """Ambil/set proxy untuk thread (round-robin; satu proxy per thread)."""
    if getattr(_PROXY_STATE, "opener", None) is not None:
        return _PROXY_STATE.opener
    px = _next_proxy()
    _PROXY_STATE.opener = _proxy_opener(px)
    _PROXY_STATE.spec = px
    return _PROXY_STATE.opener

def current_proxy_spec():
    return getattr(_PROXY_STATE, "spec", None) or "direct"


def http(method, url, data=None, headers=None, timeout=30):
    body = None
    if data is not None:
        if isinstance(data, (dict, list)):
            body = json.dumps(data).encode()
        elif isinstance(data, str):
            body = data.encode()
        else:
            body = data
    req = urllib.request.Request(url, data=body, method=method, headers={
        "User-Agent": UA,
        "Accept": "application/json",
        **({"Content-Type": "application/json"} if body else {}),
        **(headers or {}),
    })
    opener = proxy_for_thread()
    try:
        with opener.open(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw.decode())
            except Exception:
                return r.status, raw.decode()
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return -1, f"{type(e).__name__}: {e}"


def verify_email(email):
    """Lolos/tidak nya email di filter gonka."""
    s, j = http("POST", f"{FUNCTIONS_URL}/validate-email",
                {"email": email},
                {"Authorization": f"Bearer {anon_key()}", "apikey": anon_key()})
    return j


# ──────────────── inbox GrabMail ────────────────

def gm_read(address):
    """List pesan + baca isi penuh tiap pesan (untuk ambil link verify)."""
    url = "https://grabmail.io/api/v1/mailbox?" + urllib.parse.urlencode({"address": address})
    s, j = http("GET", url, headers={"User-Agent": UA})
    if not isinstance(j, dict):
        return {"messages": []}
    out = {"messages": []}
    for m in j.get("messages", []) or []:
        mid = m.get("id")
        rec = {**m, "text": "", "html": ""}
        if mid:
            u2 = (f"https://grabmail.io/api/v1/message/{mid}?"
                  + urllib.parse.urlencode({"mailbox": address}))
            s2, j2 = http("GET", u2, headers={"User-Agent": UA})
            if isinstance(j2, dict):
                rec["text"] = j2.get("text", "") or ""
                rec["html"] = j2.get("html", "") or ""
        out["messages"].append(rec)
    return out


def extract_confirm_link(messages):
    """Cari link verifikasi Supabase (auth/v1/verify?token=...) dari isi pesan."""
    for m in messages or []:
        blob = " ".join(str(m.get(k, "")) for k in ("text", "html", "subject", "body", "content"))
        # link verify Supabase
        m2 = re.search(r'(https?://[^\s"<>]+auth/v1/verify\?[^\s"<>]+)', blob)
        if m2 and m2.group(1):
            return m2.group(1).replace("&amp;", "&")
        # fallback: link redirect yang mungkin membawa token
        m3 = re.search(r'(https?://[^\s"<>]+(?:confirm|verify|confirmation)[^\s"<>]*)', blob, re.I)
        if m3 and m3.group(1):
            return m3.group(1).replace("&amp;", "&")
    return None


def poll_confirm_link(address, timeout=240):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            data = gm_read(address)
        except Exception:
            data = {}
        link = extract_confirm_link(data.get("messages", []))
        if link and "verify" in link:
            return link
        time.sleep(5)
    return None


# ──────────────── akun / key ────────────────

def newpass():
    return "Gn" + "".join(random.choices(string.ascii_letters + string.digits, k=10)) + "!x9"


def farm_one(idx):
    # reset proxy thread-local agar tiap akun dapat proxy baru (rotasi round-robin)
    _PROXY_STATE.opener = None
    _PROXY_STATE.spec = None
    proxy_for_thread()   # tetapkan proxy utk akun ini
    rec = {"index": idx, "status": "failed", "email": "", "api_key": "",
           "error": "", "proxy": current_proxy_spec()}
    try:
        # 1) inbox GrabMail
        addr = f"gonka{idx}{''.join(random.choices(string.digits, k=4))}@grabmail.io"
        rec["email"] = addr
        print(f"  [{idx}] inbox: {addr}", flush=True)
        pwd = newpass()

        # 2) validasi (sanity)
        v = verify_email(addr)
        if not (isinstance(v, dict) and v.get("ok")):
            rec["error"] = f"email ditolak: {v}"
            return rec

        # 3) signup dengan ref_code
        s, j = http("POST", f"{AUTH_URL}/signup",
                    {"email": addr, "password": pwd,
                     "data": {"ref_code": REF_CODE}},
                    {"apikey": anon_key(), "Authorization": f"Bearer {anon_key()}"})
        if s not in (200, 201) or not isinstance(j, dict) or "id" not in j:
            rec["error"] = f"signup {s}: {str(j)[:160]}"
            return rec
        user_id = j.get("id")
        print(f"  [{idx}] akun dibuat ({user_id[:8]}), tunggu email konfirmasi...", flush=True)

        # 4) baca link konfirmasi
        link = poll_confirm_link(addr, 240)
        if not link:
            rec["error"] = "tidak dapat link konfirmasi (timeout)"
            return rec
        print(f"  [{idx}] link konfirmasi diterima", flush=True)

        # 5) buka link verifikasi — Supabase menerima GET dengan token di query
        v_url = link
        if "apikey" not in v_url:
            sep = "&" if "?" in link else "?"
            v_url = f"{link}{sep}apikey={anon_key()}"
        s2, _ = http("GET", v_url, headers={"apikey": anon_key(), "Authorization": f"Bearer {anon_key()}"})
        if s2 not in (200, 301, 302):
            rec["error"] = f"verifikasi HTTP {s2}"
            return rec
        print(f"  [{idx}] terverifikasi (HTTP {s2})", flush=True)

        # 6) login -> JWT (JSON body di URL ?grant_type=password — sudah teruji)
        req = urllib.request.Request(f"{AUTH_URL}/token?grant_type=password",
                                     data=json.dumps({"email": addr, "password": pwd}).encode(),
                                     method="POST",
                                     headers={"apikey": anon_key(),
                                              "Content-Type": "application/json",
                                              "User-Agent": UA})
        opener = proxy_for_thread()
        try:
            with opener.open(req, timeout=30) as r:
                t = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            try:
                t = json.loads(e.read().decode())
            except Exception:
                t = {}
        if not isinstance(t, dict) or "access_token" not in t:
            rec["error"] = f"login: {str(t)[:160]}"
            return rec
        jwt = t["access_token"]
        print(f"  [{idx}] login ok (JWT {len(jwt)} char)", flush=True)

        # 7) INSERT api_keys
        s4, k = http("POST", f"{REST_URL}/api_keys",
                     {"user_id": user_id},
                     {"apikey": anon_key(), "Authorization": f"Bearer {jwt}",
                      "Content-Type": "application/json", "Prefer": "return=representation"})
        print(f"  [{idx}] insert api_keys HTTP {s4}: {str(k)[:200]}", flush=True)

        key = None
        if isinstance(k, list) and k:
            key = k[0].get("key")
        if not key and isinstance(k, dict):
            key = k.get("key")
        if not key:
            # coba SELECT kembali
            s5, g = http("GET", f"{REST_URL}/api_keys?select=key&user_id=eq.{user_id}",
                         headers={"apikey": anon_key(),
                                  "Authorization": f"Bearer {jwt}"})
            if isinstance(g, list) and g:
                key = g[0].get("key")
        if not key:
            rec["error"] = f"key tidak didapat (insert {s4}: {str(k)[:120]})"
            return rec

        rec["status"] = "success"
        rec["api_key"] = key
        with FILE_LOCK:
            with open(OUT_KEYS, "a", encoding="utf-8") as f:
                f.write(key + "\n")
        print(f"  [{idx}] ✔ KEY: {key[:24]}...", flush=True)
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {str(e)[:150]}"
        print(f"  [{idx}] ✖ EXC {rec['error'][:110]}", flush=True)

    with FILE_LOCK:
        with open(OUT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    return rec


def cmd_check():
    print("Konfigurasi:")
    print(f"  Supabase : {SUPABASE}")
    print(f"  Referral : {REF_CODE}")
    try:
        v = verify_email(f"probe.{random.randint(1000,9999)}@grabmail.io")
        print(f"  validate grabmail.io : {v}")
    except Exception as e:
        print(f"  validate : ERR {e}")
    return 0


def load_proxies(args):
    """Isi _PROXY_LIST dari --proxy atau --proxy-file. None = direct."""
    items = []
    if args.proxy:
        items.append(args.proxy)
    if args.proxy_file:
        try:
            with open(args.proxy_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        items.append(line)
        except FileNotFoundError:
            print(f"✖ proxy file tidak ada: {args.proxy_file}")
    _PROXY_LIST[:] = items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("count", nargs="?", type=int, default=None)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--sleep", type=float, default=0, help="jeda antar akun (detik)")
    ap.add_argument("-w", "--workers", type=int, default=1, help="jumlah worker paralel")
    ap.add_argument("--proxy", default=None, help="proxy tunggal, format user:pass@host:port atau host:port")
    ap.add_argument("--proxy-file", default=None, help="file berisi daftar proxy (1 per baris), diputar round-robin")
    ap.add_argument("--ref", default=REF_CODE)
    args = ap.parse_args()

    if args.ref:
        globals()["REF_CODE"] = args.ref
    load_proxies(args)

    if args.check:
        return cmd_check()

    n = args.count or 1
    print(f"GonkaAPI Farm | {n} akun | workers {args.workers} | "
          f"proxy-file {len(_PROXY_LIST)} | ref {REF_CODE}")
    ok = 0
    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(farm_one, i): i for i in range(1, n + 1)}
            for f in as_completed(futs):
                if f.result().get("status") == "success":
                    ok += 1
    else:
        for i in range(1, n + 1):
            r = farm_one(i)
            if r["status"] == "success":
                ok += 1
            if i < n and args.sleep:
                time.sleep(args.sleep)
    print(f"\n=== {ok}/{n} berhasil -> {OUT_KEYS} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
