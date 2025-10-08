#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hash_comparativo.py — pipeline completo

Incluye:
- Descargas: Tevelam/Disco/ARS (requests) + IMSA (Selenium)
- SHA del archivo completo por proveedor (si es igual, omite todo)
- DB pública exacta + meta (public_db/<PROV>_DB.xlsx + .meta.json)
- Stock V (public_listas/<PROV>_ULTIMA.xlsx) con políticas por proveedor:
    * Tevelam / Disco_Pro: respeta “Mayor a 5 / Menor a 5”; si hay número usa umbral >=5
    * IMSA: solo texto positivo (“con stock”, “en stock”, “disponible”)
    * ARS_Tech: numérico > 0 (o texto positivo)
- Diffs de precios/modelos respecto del snapshot anterior (_db/snapshots/<PROV>_snapshot.csv)
    * Genera public_reports/<PROV>_DIFF_YYYYMMDD_HHMM.xlsx si hay cambios
- Índices JSON para la web (public_reports/index.json y public_listas/index.json)

Notas:
- Para probar con un archivo editado a mano sin depender de la descarga:
    TEVELAM_FORCE=path/a/tu_archivo.xlsx python hash_comparativo.py
    (Existen también DISCO_FORCE, ARS_FORCE, IMSA_FORCE)
"""

from __future__ import annotations
import os, re, json, time, hashlib, shutil
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass

import requests
import pandas as pd
from openpyxl import Workbook

# Selenium (IMSA)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ================== Config base ==================
BASE_DIR = Path(os.getenv("WORKDIR", ".")).resolve()
AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

# Estado interno
DB_STATE_DIR  = BASE_DIR / "_db"
HASH_DIR      = DB_STATE_DIR / "hash"
SNAP_DIR      = DB_STATE_DIR / "snapshots"

# Publicación (Pages)
PUBLIC_DB_DIR      = BASE_DIR / "public_db"
PUBLIC_LISTAS_DIR  = BASE_DIR / "public_listas"
PUBLIC_REPORTS_DIR = BASE_DIR / "public_reports"

for d in (DB_STATE_DIR, HASH_DIR, SNAP_DIR, PUBLIC_DB_DIR, PUBLIC_LISTAS_DIR, PUBLIC_REPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Ventana horaria queda en el workflow (guard bash).
# Si quisieras también acá: setea AR_WINDOW=off para deshabilitar.
AR_WINDOW = os.getenv("AR_WINDOW", "off").lower()  # "off" por defecto

# Si el SHA es igual → borrar descargado duplicado
BORRAR_DUPLICADO = os.getenv("BORRAR_DUPLICADO", "true").lower() == "true"

# URLs fuentes (las que ya venías usando)
URL_TEVELAM         = "https://drive.google.com/uc?export=download&id=1hPH3VwQDtMgx_AkC5hFCUbM2MEiwBEpT"
URL_DISCO_PRO       = "https://drive.google.com/uc?id=1-aQ842Dq3T1doA-Enb34iNNzenLGkVkr&export=download"
URL_ARS_TECH        = "https://docs.google.com/uc?id=1JnUnrpZUniTXUafkAxCInPG7O39yrld5&export=download"

IMSA_URL      = "https://listaimsa.com.ar/lista-de-precios/"
IMSA_PASSWORD = os.getenv("IMSA_PASSWORD", "lista2021")

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
TIMEOUT = 60

# ---------------------- Logging ----------------------
def now_utc() -> datetime:   return datetime.now(timezone.utc)
def now_ar()  -> datetime:   return datetime.now(AR_TZ)
def log(msg: str) -> None:   print(f"[{now_ar().strftime('%H:%M:%S')}] {msg}")
def ts_name() -> str:        return now_utc().strftime("%Y%m%d_%H%M")

# ---------------------- SHA helpers ----------------------
def sha_path(source_key: str) -> Path:
    return HASH_DIR / f"{source_key}.sha256"

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def read_prev_hash(source_key: str) -> str|None:
    p = sha_path(source_key)
    if p.exists():
        try: return p.read_text(encoding="utf-8").strip()
        except: return None
    return None

def write_hash(source_key: str, hexhash: str) -> None:
    sha_path(source_key).write_text(hexhash, encoding="utf-8")

def decide_should_process(source_key: str, path: Path|None) -> tuple[bool,str|None]:
    if not path or not path.exists():
        log(f"⏭️ {source_key}: no hay archivo para comparar.")
        return False, None
    try:
        new_hash = file_sha256(path)
        prev_hash = read_prev_hash(source_key)
        log(f"📇 {source_key}: nuevo={new_hash[:12]}… | previo={(prev_hash[:12]+'…') if prev_hash else 'N/A'}")
        if prev_hash == new_hash:
            log(f"⏭️ {source_key}: sin cambios (SHA igual) → omito.")
            if BORRAR_DUPLICADO:
                try: path.unlink(missing_ok=True); log(f"🗑️ {source_key}: duplicado borrado: {path.name}")
                except Exception as e: log(f"⚠️ {source_key}: no se pudo borrar duplicado: {e}")
            return False, new_hash
        write_hash(source_key, new_hash)
        log(f"🔄 {source_key}: cambios detectados (SHA distinto) → proceso.")
        return True, new_hash
    except Exception as e:
        log(f"⚠️ {source_key}: error comparando hash ({e}) → proceso por las dudas.")
        return True, None

# ---------------------- Descargas ----------------------
DOWNLOADS = BASE_DIR/"downloads"; DOWNLOADS.mkdir(exist_ok=True, parents=True)

def download_simple(url: str, base_name: str) -> Path|None:
    try:
        dst = DOWNLOADS / f"{base_name}_{ts_name()}.xlsx"
        r = requests.get(url, headers=REQ_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        dst.write_bytes(r.content)
        log(f"✅ Descargado: {dst.name}")
        return dst
    except Exception as e:
        log(f"❌ Error descarga {base_name}: {e}")
        return None

def build_chrome() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    prefs = {"download.default_directory": str(DOWNLOADS),
             "download.prompt_for_download": False,
             "safebrowsing.enabled": True}
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
    return driver

def close_driver(drv):
    try: drv.quit()
    except: pass

def find_recent_imsa(max_age=300) -> Path|None:
    now = time.time()
    cands=[]
    for p in DOWNLOADS.glob("*.xls*"):
        n=p.name.lower()
        if "lista" in n or "imsa" in n:
            if now - p.stat().st_mtime <= max_age and not (DOWNLOADS/(p.name+".crdownload")).exists():
                cands.append(p)
    return max(cands, key=lambda x:x.stat().st_mtime) if cands else None

def descargar_imsa_web() -> Path|None:
    drv=None
    try:
        drv=build_chrome()
        log("🌐 Abriendo IMSA…")
        drv.get(IMSA_URL)
        try:
            ifr = WebDriverWait(drv, 20).until(EC.presence_of_all_elements_located((By.TAG_NAME,"iframe")))
            drv.switch_to.frame(ifr[0])
            WebDriverWait(drv, 20).until(EC.element_to_be_clickable((By.ID,"pass"))).send_keys(IMSA_PASSWORD)
            btn = drv.find_element(By.XPATH, "//input[@type='submit' and @value='Login']")
            drv.execute_script("arguments[0].click();", btn)
            log("🔑 Login enviado.")
        except Exception as e:
            log(f"⚠️ IMSA: no se pudo automatizar login ({e})")
        finally:
            try: drv.switch_to.default_content()
            except: pass
        log("⏳ Espera 60s descarga…")
        time.sleep(60)
        cand=find_recent_imsa(420)
        if cand:
            log(f"✅ IMSA detectado: {cand.name}")
            return cand
        # polling extra 60s
        deadline=time.time()+60
        while time.time()<deadline:
            cand=find_recent_imsa(420)
            if cand:
                log(f"✅ IMSA detectado: {cand.name}")
                return cand
            time.sleep(2)
        log("⚠️ IMSA no detectado.")
        return None
    except Exception as e:
        log(f"❌ Error IMSA: {e}")
        return None
    finally:
        if drv: close_driver(drv); log("🧹 Selenium cerrado.")

# ================== Parseo + políticas ==================
CONFIG = {
    "Tevelam":   {"file_glob":"Tevelam_*.xlsx","db_name":"Tevelam_DB.xlsx","lista_name":"Tevelam_ULTIMA.xlsx",
                  "prefer_pvp":True,"header_anchor":r"^codigo$",
                  "stock_policy":{"type":"min_qty","min_qty":5},  # respeta mayor/menor a 5
                  "cols":{
                      "codigo":[r"^cod(?:igo)?$"],"marca":[r"^marca$"],"modelo":[r"^(nombre|modelo|producto)$"],
                      "categoria":[r"^categor"],"pvc":[r"^(p\.?v\.?c\.?|pvc|precio\s*lista|lista)$"],
                      "iva":[r"^(impuestos|iva)$"],"pvp":[r"^(pvp|precio\s*p(ú|u)blico|precio\s*final|subtotal)$"],
                      "stock":[r"^stock$"]}},
    "Disco_Pro": {"file_glob":"Disco_Pro_*.xlsx","db_name":"Disco_Pro_DB.xlsx","lista_name":"Disco_Pro_ULTIMA.xlsx",
                  "prefer_pvp":True,"header_anchor":r"^codigo$",
                  "stock_policy":{"type":"min_qty","min_qty":5},  # respeta mayor/menor a 5
                  "cols":{
                      "codigo":[r"^cod(?:igo)?$"],"marca":[r"^marca$"],"modelo":[r"^(nombre|modelo|descrip|producto)$"],
                      "categoria":[r"^categor"],"pvc":[r"^(pvc|precio\s*lista|lista)$"],
                      "iva":[r"^(impuestos|iva)$"],"pvp":[r"^(pvp|precio\s*final|precio\s*p(ú|u)blico)$"],
                      "stock":[r"^stock$"]}},
    "ARS_Tech":  {"file_glob":"ARS_Tech_*.xlsx","db_name":"ARS_Tech_DB.xlsx","lista_name":"ARS_Tech_ULTIMA.xlsx",
                  "prefer_pvp":True,"header_anchor":r"^codigo$",
                  "stock_policy":{"type":"min_qty","min_qty":1},  # >0
                  "cols":{
                      "codigo":[r"^cod(?:igo)?$"],"marca":[r"^marca$"],"modelo":[r"^(nombre|modelo|descrip|producto)$"],
                      "categoria":[r"^categor"],"pvc":[r"^(pvc|precio\s*lista|lista)$"],
                      "iva":[r"^(impuestos|iva)$"],"pvp":[r"^(pvp|precio\s*final|precio\s*p(ú|u)blico)$"],
                      "stock":[r"^stock$"]}},
    "IMSA":      {"file_glob":"ListaImsa*.xlsx","db_name":"IMSA_DB.xlsx","lista_name":"IMSA_ULTIMA.xlsx",
                  "prefer_pvp":True,"header_anchor":r"^codigo$",
                  "stock_policy":{"type":"text_only"},         # solo texto positivo
                  "cols":{
                      "codigo":[r"^cod(?:igo)?$"],"marca":[r"^marca$"],"modelo":[r"^(nombre|modelo|descrip|producto)$"],
                      "categoria":[r"^categor"],"pvc":[r"^(pvc|precio\s*lista|lista)$"],
                      "iva":[r"^(impuestos|iva)$"],"pvp":[r"^(pvp|precio\s*final|precio)$"],
                      "stock":[r"^stock$"]}},
}

@dataclass
class Parsed:
    df_clean: pd.DataFrame
    used_cols: dict
    header_row: int

_num_re = re.compile(r"[-+]?\d[\d.,]*")
def to_num(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s=str(x).strip()
    if not s: return None
    if s.endswith("%"):
        m=_num_re.search(s); 
        if not m: return None
        v=m.group(0).replace(".","").replace(",",".")
        try: return float(v)/100.0
        except: return None
    m=_num_re.search(s)
    if not m: return None
    v=m.group(0).replace(".","").replace(",",".")
    try: return float(v)
    except: return None

def read_any_excel(xls_path: Path, header_row: int|None, anchor_regex: str|None):
    if header_row is None:
        df0 = pd.read_excel(xls_path, sheet_name=0, header=None)
        rx = re.compile(anchor_regex or r"^codigo$", re.I)
        header_row = 0
        for i in range(min(300, len(df0))):
            vals = [str(v).strip().lower() for v in df0.iloc[i].tolist() if pd.notna(v)]
            if any(rx.search(v) for v in vals): header_row = i; break
    df = pd.read_excel(xls_path, sheet_name=0, header=header_row)
    return df, header_row

def pick_col(cols, patterns):
    for pat in patterns:
        rx = re.compile(pat, re.I)
        for c in cols:
            if rx.search(str(c)): return c
    return None

# ----- políticas de stock -----
POS_TEXT = ["con stock","en stock","disponible","disponibles","hay stock","hay","sí","si","ok","true"]
NEG_TEXT = ["sin stock","agotado","no hay","no","false","cero","0"]
_mayor_5 = re.compile(r"(mayor\s*(a|que)\s*5|>\s*5)", re.I)
_menor_5 = re.compile(r"(menor\s*(a|que)\s*5|<\s*5)", re.I)

def text_has_any(s: str, bag: list[str]) -> bool:
    if s is None: return False
    t=str(s).strip().lower()
    return any(tok in t for tok in bag)

def provider_has_stock(provider: str, cfg: dict, stock_text, stock_num) -> bool:
    t = ("" if stock_text is None else str(stock_text)).strip().lower()
    if text_has_any(t, NEG_TEXT): return False

    if provider in ("Tevelam","Disco_Pro"):
        if _mayor_5.search(t): return True
        if _menor_5.search(t): return False
        if stock_num is not None:
            try: return float(stock_num) >= 5
            except: pass
        return text_has_any(t, POS_TEXT)

    if provider == "IMSA":
        return text_has_any(t, POS_TEXT)

    # ARS_Tech y default
    if stock_num is not None:
        try: return float(stock_num) > 0
        except: pass
    return text_has_any(t, POS_TEXT)

def parse_with_config(provider: str, xls_path: Path, cfg: dict) -> Parsed:
    df_raw, hdr = read_any_excel(xls_path, header_row=None, anchor_regex=cfg.get("header_anchor"))
    cols = list(df_raw.columns)
    def col(key): return pick_col(cols, cfg["cols"].get(key, []))
    c_codigo=col("codigo"); c_marca=col("marca"); c_modelo=col("modelo")
    c_cat=col("categoria"); c_pvc=col("pvc"); c_iva=col("iva"); c_pvp=col("pvp"); c_stock=col("stock")
    needed_any_price = c_pvp or c_pvc
    if any(v is None for v in [c_codigo, c_marca, c_modelo, needed_any_price]):
        raise RuntimeError(f"[{provider}] No pude mapear columnas mínimas. Columnas={cols}")

    keep=[x for x in [c_codigo,c_marca,c_modelo,c_cat,c_pvc,c_iva,c_pvp,c_stock] if x]
    df = df_raw[keep].copy()
    df.rename(columns={
        c_codigo:"codigo", c_marca:"marca", c_modelo:"modelo",
        (c_cat or "categoria"):"categoria", (c_pvc or "pvc"):"pvc",
        (c_iva or "iva"):"iva", (c_pvp or "pvp"):"pvp", (c_stock or "stock"):"stock"
    }, inplace=True)

    df["pvc_num"] = df["pvc"].map(to_num) if "pvc" in df.columns else None
    df["iva_num"] = df["iva"].map(to_num) if "iva" in df.columns else None
    df["pvp_num"] = df["pvp"].map(to_num) if "pvp" in df.columns else None
    df["stock_num"] = df["stock"].map(to_num) if "stock" in df.columns else None

    def precio_final(row):
        if cfg.get("prefer_pvp") and pd.notna(row.get("pvp_num")):
            return row["pvp_num"]
        pvc=row.get("pvc_num"); iva=row.get("iva_num")
        if pd.notna(pvc) and pd.notna(iva): return round(float(pvc)*(1+float(iva)),2)
        if pd.notna(pvc): return float(pvc)
        if pd.notna(row.get("pvp_num")): return row["pvp_num"]
        return None
    df["precio_final"]=df.apply(precio_final,axis=1)

    for c in ["codigo","marca","modelo","categoria"]:
        if c in df.columns: df[c]=df[c].astype(str).str.strip()
        else: df[c]=None

    df = df[df.apply(lambda r: provider_has_stock(provider, cfg, r.get("stock"), r.get("stock_num")), axis=1)]
    df = df[df["codigo"].notna() & df["codigo"].ne("") & df["precio_final"].notna()].copy()
    df = df[["codigo","marca","modelo","categoria","precio_final","stock"]]

    used = {"codigo":c_codigo,"marca":c_marca,"modelo":c_modelo,"categoria":c_cat,
            "pvc":c_pvc,"iva":c_iva,"pvp":c_pvp,"stock":c_stock}
    return Parsed(df, used, hdr)

# ================== Diffs & snapshots ==================
def load_snapshot(p: Path) -> pd.DataFrame|None:
    if not p.exists(): return None
    try: return pd.read_csv(p, dtype={"codigo":str})
    except: return None

def save_snapshot(df: pd.DataFrame, p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    df[["codigo","modelo","precio_final"]].to_csv(p, index=False)

def diff_prices(new_df, old_df) -> pd.DataFrame:
    if old_df is None or old_df.empty: return pd.DataFrame()
    a = old_df[["codigo","modelo","precio_final"]].rename(columns={"modelo":"modelo_ant","precio_final":"precio_ant"})
    b = new_df[["codigo","modelo","precio_final"]].rename(columns={"modelo":"modelo_nuevo","precio_final":"precio_nuevo"})
    m = a.merge(b, on="codigo", how="outer", indicator=True)
    def changed(r):
        if r["_merge"]!="both": return True
        pa, pn = r["precio_ant"], r["precio_nuevo"]
        if pd.isna(pa) and pd.isna(pn): return False
        if pd.isna(pa)!=pd.isna(pn): return True
        try: return abs(float(pn)-float(pa))>0.005
        except: return True
    m["cambio"]=m.apply(changed,axis=1)
    out=m[m["cambio"]].copy()
    def kind(r):
        if r["_merge"]=="both": return 0
        if r["_merge"]=="right_only": return 1
        return 2
    out["tipo"]=out.apply(kind,axis=1)
    out.sort_values(["tipo","codigo"],inplace=True)
    out.rename(columns={"_merge":"estado(ambas/solo_nuevo/solo_anterior)"},inplace=True)
    return out[["codigo","modelo_ant","precio_ant","modelo_nuevo","precio_nuevo","estado(ambas/solo_nuevo/solo_anterior)"]]

# ================== Publicación ==================
def save_excel(df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Hoja1")

def publish_db_copy(provider: str, src: Path, sha: str|None):
    dst_pub  = PUBLIC_DB_DIR / CONFIG[provider]["db_name"]
    dst_state= DB_STATE_DIR   / CONFIG[provider]["db_name"]
    shutil.copy2(src, dst_pub)
    shutil.copy2(src, dst_state)
    meta = {"vendor":provider,"saved_at_ar":now_ar().isoformat(timespec="seconds"),
            "sha256": (sha or file_sha256(src)),"source_name":src.name,"size_bytes":src.stat().st_size}
    for root in (PUBLIC_DB_DIR, DB_STATE_DIR):
        (root/CONFIG[provider]["db_name"].replace(".xlsx",".meta.json")).write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

def build_indexes():
    # public_reports/index.json
    items=[]
    for p in PUBLIC_REPORTS_DIR.glob("*.xlsx"):
        st=p.stat()
        items.append({"name":p.name,"url":f"public_reports/{p.name}",
                      "size_kb":round(st.st_size/1024,1),"mtime":int(st.st_mtime)})
    items.sort(key=lambda x:x["mtime"], reverse=True)
    (PUBLIC_REPORTS_DIR/"index.json").write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")
    # public_listas/index.json
    items=[]
    for p in PUBLIC_LISTAS_DIR.glob("*_ULTIMA.xlsx"):
        st=p.stat()
        items.append({"name":p.name,"url":f"public_listas/{p.name}",
                      "size_kb":round(st.st_size/1024,1),"mtime":int(st.st_mtime)})
    items.sort(key=lambda x:x["mtime"], reverse=True)
    (PUBLIC_LISTAS_DIR/"index.json").write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")

# ================== Pipeline proveedor ==================
def newest_or_public(provider:str, force_env_var:str|None) -> Path|None:
    # 1) FORCE
    if force_env_var:
        v=os.environ.get(force_env_var,"").strip()
        if v:
            p=Path(v); 
            if p.exists(): return p
    # 2) último descargado
    glb=CONFIG[provider]["file_glob"]
    cands=sorted(DOWNLOADS.glob(glb), key=lambda p: p.stat().st_mtime, reverse=True)
    if cands: return cands[0]
    # 3) fallback: DB pública (sirve para primer snapshot)
    p=PUBLIC_DB_DIR/CONFIG[provider]["db_name"]
    return p if p.exists() else None

def process_provider(provider: str):
    force_var = {"Tevelam":"TEVELAM_FORCE","Disco_Pro":"DISCO_FORCE","ARS_Tech":"ARS_FORCE","IMSA":"IMSA_FORCE"}[provider]
    src = newest_or_public(provider, force_var)
    if not src:
        log(f"[{provider}] No hay archivo para procesar."); return

    debe, sha_hex = decide_should_process(provider, src)
    if not debe: return

    # Parse + filtros + precio final
    try:
        parsed = parse_with_config(provider, src, CONFIG[provider])
    except Exception as e:
        log(f"[{provider}] ERROR parseando: {e}")
        return

    # Stock V
    hoja = parsed.df_clean.rename(columns={
        "codigo":"Código","marca":"Marca","modelo":"Modelo","categoria":"Categoría","precio_final":"Precio","stock":"Stock"
    })
    out_lista = PUBLIC_LISTAS_DIR/CONFIG[provider]["lista_name"]
    save_excel(hoja, out_lista)
    log(f"[{provider}] ✅ Stock V → {out_lista.name}  filas={len(hoja)}")

    # Diffs
    snap = SNAP_DIR/f"{provider}_snapshot.csv"
    old = load_snapshot(snap)
    dif = diff_prices(parsed.df_clean, old)
    if old is None:
        log(f"[{provider}] ℹ️ Primer snapshot → sin reporte.")
    elif not dif.empty:
        out = PUBLIC_REPORTS_DIR/f"{provider}_DIFF_{ts_name()}.xlsx"
        save_excel(dif, out)
        log(f"[{provider}] 🧾 Reporte de cambios → {out.name}  filas={len(dif)}")
    else:
        log(f"[{provider}] ℹ️ Sin cambios de precio/modelos.")

    # DB exacta + meta
    publish_db_copy(provider, src, sha_hex)

    # Persistencia
    save_snapshot(parsed.df_clean, snap)

# ================== Main ==================
def guard_ar_window():
    if AR_WINDOW != "on":  # por defecto off; el guard real está en el workflow
        return True
    now = now_ar()
    dow = now.isoweekday()  # 1=Lunes .. 7=Domingo
    h = now.hour
    if dow>5: return False
    if h<7 or h>18: return False
    return True

def main():
    log("INICIO: descarga + SHA + DB pública + Stock V + difs + índices")
    if not guard_ar_window():
        log("Fuera de ventana AR (L–V 07–18). Salgo.")
        return

    # Descargas (si no usás FORCE)
    tevelam = newest_or_public("Tevelam","TEVELAM_FORCE") or download_simple(URL_TEVELAM, "Tevelam")
    disco   = newest_or_public("Disco_Pro","DISCO_FORCE") or download_simple(URL_DISCO_PRO, "Disco_Pro")
    ars     = newest_or_public("ARS_Tech","ARS_FORCE")    or download_simple(URL_ARS_TECH,  "ARS_Tech")

    # IMSA
    imsa = newest_or_public("IMSA","IMSA_FORCE")
    if not imsa:
        try:
            log("Lanzando IMSA…")
            imsa = descargar_imsa_web()
        except Exception as e:
            log(f"ERROR IMSA: {e}")

    log("Procesando por proveedor (según SHA)…")
    if tevelam: process_provider("Tevelam")
    if disco:   process_provider("Disco_Pro")
    if ars:     process_provider("ARS_Tech")
    if imsa:    process_provider("IMSA")

    build_indexes()
    log("FIN.")

if __name__ == "__main__":
    import time as _time
    main()
