#!/usr/bin/env python3
"""filter_proxies.py — Filter proxy yang aktif, cepat, DAN BISA AKSES target GonkaAI.

Meniru pola dahl-farm: proxy yang lolos health-check terhadap TARGET_URL
(GonkaAPI + Supabase) disimpan ke 'proxies_good.txt' dalam waktu nyata.
Dukungan input file (proxies_all.txt, data.txt, proxies.txt) atau auto-scrape
dari sumber publik. Zero-dep mode dasar (urllib), pakai httpx bila ada.

Pakai:
  python filter_proxies.py 60          # cari 60 proxy yang bisa akses gonka
  python filter_proxies.py 60 data.txt # dari file proxy mentah
  python filter_proxies.py 60 proxies.txt
"""
from __future__ import annotations

import os
import sys
import time
import urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

TARGET = os.getenv("GONKA_TARGET",
                   "https://hskyauefqcgbvgvxkluj.supabase.co/functions/v1/validate-email")
TIMEOUT = 5.0
MAX_WORKERS = 80

GRN = "\033[32m"; RED = "\033[31m"; YEL = "\033[33m"
CYN = "\033[36m"; DIM = "\033[2m"; RST = "\033[0m"

PUBLIC_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all&ssl=all&anonymity=all",
]


def auto_scrape_proxies():
    print(f"  {YEL}[*] Mengunduh proxy publik gratis dari beberapa sumber...{RST}")
    scraped = set()
    for src in PUBLIC_SOURCES:
        try:
            req = urllib.request.Request(src, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                text = resp.read().decode("utf-8", errors="ignore")
                for line in text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "://" not in line:
                            # sesuaikan proto dgn nama sumber
                            if "socks" in src.split("/")[-1] or "socks5" in src.lower() or "socks" in src.lower():
                                line = f"socks5://{line}"
                            else:
                                line = f"http://{line}"
                        scraped.add(line)
        except Exception:
            pass
    print(f"  {GRN}[V] Berhasil mengumpulkan {len(scraped):,} proxy publik gratis!{RST}\n")
    return list(scraped)


def normalize(p):
    p = p.strip()
    if not p or p.startswith("#"):
        return None
    if "://" not in p:
        parts = p.split(":")
        if len(parts) == 4:
            ip, port, user, pwd = parts
            return f"http://{user}:{pwd}@{ip}:{port}"
        return f"http://{p}"
    return p


def check_proxy(proxy_url):
    """Proxy hidup DAN bisa akses target gonka/Supabase → (url, latency, status)."""
    p = normalize(proxy_url)
    if not p:
        return None
    t0 = time.time()
    try:
        import httpx
        with httpx.Client(proxy=p, timeout=httpx.Timeout(TIMEOUT, connect=TIMEOUT / 2),
                          follow_redirects=False, verify=False) as c:
            r = c.post(TARGET, json={"email": "probe@example.com"},
                       headers={"User-Agent": "Mozilla/5.0",
                                "Content-Type": "application/json",
                                "Origin": "https://gonka-api.org"})
            latency = (time.time() - t0) * 1000
            if r.status_code in (400, 422, 200, 201):
                return p, latency, r.status_code
            if r.status_code == 429:
                return None  # IP proxy sudah kena rate-limit -> reject
    except Exception:
        pass
    # fallback zero-dep (urllib) bila httpx tidak ada
    try:
        data = b'{"email":"probe@example.com"}'
        proxy_op = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": p, "https": p}))
        req = urllib.request.Request(TARGET, data=data, method="POST", headers={
            "User-Agent": "Mozilla/5.0", "Content-Type": "application/json",
            "Origin": "https://gonka-api.org"})
        with proxy_op.open(req, timeout=TIMEOUT) as r:
            latency = (time.time() - t0) * 1000
            if r.status in (400, 422, 200, 201):
                return p, latency, r.status
            if r.status == 429:
                return None
    except Exception:
        pass
    return None


def main():
    target_count = 30
    input_file = None

    for arg in sys.argv[1:]:
        if os.path.isfile(arg):
            input_file = Path(arg)
        else:
            try:
                target_count = int(arg)
            except ValueError:
                pass

    here = Path(os.path.dirname(os.path.abspath(__file__)))
    here = Path.cwd()
    if not input_file:
        for cand in (here / "proxies_all.txt", here / "data.txt", here / "proxies.txt"):
            if cand.exists():
                input_file = cand
                break

    output_file = here / "proxies_good.txt"

    print(f"\n{CYN}========================================================{RST}")
    print(f"{CYN}        PROXY CHECKER & FILTER - GONKA FARM            {RST}")
    print(f"{CYN}========================================================{RST}")
    print(f"  Target Test  : {TARGET}")
    print(f"  (proxy harus BISA mengakses gonka-api.org + Supabase)")
    print(f"  Timeout      : {TIMEOUT}s")
    print(f"  Threads      : {MAX_WORKERS}")

    all_proxies = []
    if input_file and input_file.exists():
        print(f"  Input File   : {input_file.name}")
        with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
            all_proxies = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    else:
        print(f"  Input File   : {YEL}Auto-Scrape (Online Sources){RST}")
        all_proxies = auto_scrape_proxies()
        raw_file = here / "proxies_all.txt"
        raw_file.write_text("\n".join(all_proxies), encoding="utf-8")
        print(f"  {DIM}Daftar mentah disimpan ke {raw_file.name}{RST}")

    seen, deduped = set(), []
    for p in all_proxies:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    all_proxies = deduped

    total = len(all_proxies)
    print(f"  Total Proxy  : {total:,} proxy unik dimuat.")
    print(f"  Output File  : {output_file.name}\n")

    existing_good = set()
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8", errors="ignore") as f:
            existing_good = {line.strip() for line in f if line.strip()}
        print(f"  {DIM}Sudah ada {len(existing_good)} proxy di {output_file.name}{RST}\n")

    print(f"  {YEL}Mencari {target_count} proxy yang BISA akses gonka...{RST}")
    print(f"  {DIM}(Tekan Ctrl+C untuk berhenti, hasil tersimpan otomatis){RST}\n")

    good_count = 0
    checked_count = 0
    t_start = time.time()

    with open(output_file, "a", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            proxy_iter = iter(all_proxies)
            futures = {}
            for _ in range(min(total, MAX_WORKERS * 3)):
                p = next(proxy_iter, None)
                if p:
                    futures[executor.submit(check_proxy, p)] = p

            try:
                while futures:
                    done_future = next(as_completed(futures))
                    p = futures.pop(done_future)
                    checked_count += 1

                    try:
                        res = done_future.result()
                    except Exception:
                        res = None

                    if res:
                        good_p, latency, status = res
                        if good_p not in existing_good:
                            existing_good.add(good_p)
                            good_count += 1
                            out_f.write(good_p + "\n")
                            out_f.flush()
                            sys.stdout.write(f"\r{' '*80}\r")
                            print(f"  {GRN}V [{good_count:02d}/{target_count}]{RST} {good_p}  {DIM}({latency:.0f}ms, status {status}){RST}")

                            if good_count >= target_count:
                                print(f"\n  {GRN}Target {target_count} proxy tercapai!{RST}")
                                for fut in futures:
                                    fut.cancel()
                                break

                    next_p = next(proxy_iter, None)
                    if next_p:
                        futures[executor.submit(check_proxy, next_p)] = next_p

                    if checked_count % 30 == 0:
                        sys.stdout.write(f"\r  {DIM}Memeriksa: {checked_count:,}/{total:,} | Ditemukan: {good_count} bagus | Waktu: {time.time()-t_start:.1f}s{RST} ")
                        sys.stdout.flush()

            except KeyboardInterrupt:
                print(f"\n\n  {YEL}Dihentikan pengguna. Hasil tersimpan di {output_file.name}.{RST}")

    print(f"\n\n{CYN}========================================================{RST}")
    print(f"  {GRN}Selesai!{RST}")
    print(f"  Proxy baru didapat : {good_count}")
    print(f"  Total di {output_file.name} : {len(existing_good)}")
    print(f"{CYN}========================================================{RST}\n")


if __name__ == "__main__":
    main()
