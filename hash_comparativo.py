# hash_comparativo.py — HASH por archivo completo + BASE visible + GATE diario
# + Hoja1 multi-hoja robusta + difs precios + refresco Hoja1 desde base
from __future__ import annotations

import csv
import time
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import os

import requests
from openpyxl import load_workbook, Workbook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ========= CONFIG (paths portables p/CI) =========
BASE_DIR = Path(os.getenv("WORKDIR", "."))

RUTA_DESCARGA = BASE_DIR / "downloads"
HASH_DB_DIR   = BASE_DIR / "_hashdb"
SNAP_DIR      = BASE_DIR / "_snapshots"
REPORTS_DIR   = BASE_DIR / "_reports"

# carpetas que publicamos en GitHub Pages
PUBLIC_REPORTS_DIR = BASE_DIR / "public_reports"
PUBLIC_LISTAS_DIR  = BASE_DIR / "public_listas"

# copia exacta de base por fuente (visible en web)
DB_DIR        = BASE_DIR / "_db"
PUBLIC_DB_DIR = BASE_DIR / "public_db"

for d in (RUTA_DESCARGA, HASH_DB_DIR, SNAP_DIR, REPORTS_DIR, PUBLIC_REPORTS_DIR, PUBLIC_LISTAS_DIR, DB_DIR, PUBLIC_DB_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Si el hash es igual al previo → se elimina el archivo recién bajado.
BORRAR_DUPLICADO = os.getenv("BORRAR_DUPLICADO", "true").lower() == "true"

# URLs fuentes (ajustá si cambian)
URL_TEVELAM = "https://drive.google.com/uc?export=download&id=1hPH3VwQDtMgx_AkC5hFCUbM2MEiwBEpT"
URL_DISCO_PRO = "https://drive.google.com/uc?id=1-aQ842Dq3T1doA-Enb34iNNzenLGkVkr&export=download"
URL_PROVEEDOR_EXTRA = "https://docs.google.com/uc?id=1JnUnrpZUniTXUafkAxCInPG7O39yrld5&export=download"

# IMSA con login embebido en iframe
IMSA_URL = "https://listaimsa.com.ar/lista-de-precios/"
IMSA_PASSWORD = os.getenv("IMSA_PASSWORD", "lista2021")

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
TIMEOUT = 60

# ========= LOG / UTILS =========
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ========= FECHA LOCAL ARG =========
try:
    from zoneinfo import ZoneInfo
    TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    TZ_AR = None

def hoy_ar_date() -> str:
    if TZ_AR:
        return datetime.now(TZ_AR).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")

# ========= HASH =========
def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

def _hash_path(source_key: str) -> Path:
    return HASH_DB_DIR / f"{source_key}.sha256"

def read_prev_hash(source_key: str) -> Optional[str]:
    p = _hash_path(source_key)
    if p.exists():
        try:
            return p.read_text(encoding='utf-8').strip()
        except Exception:
            return None
    return None

def write_hash(source_key: str, hexhash: str) -> None:
    _hash_path(source_key).write_text(hexhash, encoding='utf-8')

# ========= MANEJO DE BASE (COPIA EXACTA) =========
def _db_path(source_key: str) -> Path:
    return DB_DIR / f"{source_key}_DB.xlsx"

def _db_meta_path(source_key: str) -> Path:
    return DB_DIR / f"{source_key}_DB.meta.json"

def leer_db_meta(source_key: str) -> Dict[str, Any]:
    p = _db_meta_path(source_key)
    if not p.exists(): return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def escribir_db_meta(source_key: str, *, sha256: str, saved_at_utc: str) -> None:
    meta = {
        "source": source_key,
        "sha256": sha256,
        "saved_at_utc": saved_at_utc,
        "saved_at_ar": datetime.now(TZ_AR).strftime("%Y-%m-%d %H:%M:%S") if TZ_AR else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    _db_meta_path(source_key).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

def adoptar_como_base(source_key: str, downloaded: Path, sha256_hex: str) -> Path:
    dst = _db_path(source_key)
    dst.write_bytes(downloaded.read_bytes())
    escribir_db_meta(source_key, sha256=sha256_hex, saved_at_utc=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        (PUBLIC_DB_DIR / dst.name).write_bytes(dst.read_bytes())
        (PUBLIC_DB_DIR / f"{source_key}_DB.meta.json").write_text(
            _db_meta_path(source_key).read_text(encoding="utf-8"), encoding="utf-8"
        )
    except Exception as e:
        log(f"⚠️ No se pudo publicar base en public_db: {e}")
    log(f"💾 Base actualizada: {dst.name}")
    return dst

# ========= GATE DIARIO =========
def base_es_de_hoy(source_key: str) -> bool:
    meta = leer_db_meta(source_key)
    if not meta: return False
    try:
        saved_ar = meta.get("saved_at_ar", "")[:10]  # "YYYY-MM-DD ..."
        return saved_ar == hoy_ar_date()
    except Exception:
        return False

def skip_por_base_de_hoy(source_key: str) -> bool:
    if base_es_de_hoy(source_key):
        log(f"⏭️ {source_key}: base vigente es de HOY (AR) → omito descarga/búsqueda hasta mañana.")
        return True
    return False

# ========= DECISIÓN (HASH COMPLETO) =========
def decide_should_process(source_key: str, path: Optional[Path]) -> bool:
    if not path or not path.exists():
        log(f"⏭️ {source_key}: no hay archivo para comparar.")
        return False
    try:
        new_hash = file_sha256(path)
        prev_hash = read_prev_hash(source_key)
        log(f"📇 {source_key}: nuevo={new_hash[:12]}… | previo={(prev_hash[:12] + '…') if prev_hash else 'N/A'}")
        if prev_hash == new_hash:
            log(f"⏭️ {source_key}: sin cambios (hash igual) → omito proceso (luego refresco Hoja 1 desde base).")
            if BORRAR_DUPLICADO:
                try:
                    path.unlink(missing_ok=True)
                    log(f"🗑️ {source_key}: duplicado eliminado: {path.name}")
                except Exception as e:
                    log(f"⚠️ {source_key}: no se pudo borrar duplicado: {e}")
            return False
        write_hash(source_key, new_hash)
        adoptar_como_base(source_key, path, new_hash)
        log(f"🔄 {source_key}: cambios detectados → proceso.")
        return True
    except Exception as e:
        log(f"⚠️ {source_key}: error comparando hash: {e} → adopto y proceso.")
        try:
            h = file_sha256(path)
            write_hash(source_key, h)
            adoptar_como_base(source_key, path, h)
        except Exception:
            pass
        return True

# ========= DESCARGAS =========
def download_simple(url: str, base_name: str) -> Optional[Path]:
    try:
        dst = RUTA_DESCARGA / f"{base_name}_{ts()}.xlsx"
        r = requests.get(url, headers=REQ_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        dst.write_bytes(r.content)
        log(f"✅ Descargado: {dst.name}")
        return dst
    except Exception as e:
        log(f"❌ Error descarga {base_name}: {e}")
        return None

# ========= NORMALIZACIÓN =========
def _norm_text(s: Any) -> str:
    return (str(s) if s is not None else "").strip()

def _norm_text_lc(s: Any) -> str:
    return _norm_text(s).lower().replace("ó","o").replace("í","i").replace("á","a").replace("é","e").replace("ú","u")

def try_float(v):
    if v is None: return None
    s = _norm_text(v).replace(".", "").replace(",", ".") if isinstance(v, str) else v
    try: return float(s)
    except Exception: return None

# Detección de encabezados
CAND_COD   = {"codigo","código","cod","id","articulo","artículo","sku","modelo"}
CAND_STOCK = {"stock","estado","disponibilidad","disponible"}
CAND_PREC  = {"precio","p. lista","p lista","plista","lista","price","valor"}
CAND_MON   = {"moneda","currency","divisa"}

def detectar_columnas(ws, max_scan_rows: int = 60):
    for r_i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_scan_rows, values_only=True), start=1):
        tmp = {"codigo": None, "stock": None, "precio": None, "moneda": None}
        for c_i, cell in enumerate(row, start=1):
            h = _norm_text_lc(cell)
            if not h: continue
            if not tmp["codigo"] and h in CAND_COD:   tmp["codigo"] = c_i
            elif not tmp["precio"] and h in CAND_PREC: tmp["precio"] = c_i
            elif not tmp["moneda"] and h in CAND_MON:  tmp["moneda"] = c_i
            elif not tmp["stock"] and h in CAND_STOCK: tmp["stock"] = c_i
        if tmp["codigo"]:
            return r_i, tmp
    return None, {"codigo": None, "stock": None, "precio": None, "moneda": None}

# ========= NUEVO: elegir mejor hoja para STOCK =========
def elegir_hoja_stock(wb, fila_inicio_hint: int, col_stock_hint: int):
    """
    Recorre TODAS las hojas y devuelve la que más filas útiles aporte.
    Devuelve: (ws, modo, header_row, cols, filas_detectadas)
      - modo: "encabezados" o "fallback"
    """
    mejor = (None, None, None, {"codigo":None,"stock":None,"precio":None,"moneda":None}, 0)
    for ws in wb.worksheets:
        # 1) Intento por encabezados
        header_row, cols = detectar_columnas(ws)
        if header_row and cols["codigo"]:
            cnt = 0
            max_needed_col = max(v for v in cols.values() if v)
            for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                cod = row[cols["codigo"]-1] if cols["codigo"] else None
                if _norm_text(cod): cnt += 1
            if cnt > mejor[4]:
                mejor = (ws, "encabezados", header_row, cols, cnt)
            continue

        # 2) Fallback: 1ra col = código, col_stock_hint = stock
        cnt_fb = 0
        max_row = ws.max_row or 1
        for r in range(fila_inicio_hint, max_row + 1):
            cod = ws.cell(row=r, column=1).value
            if _norm_text(cod):
                cnt_fb += 1
        if cnt_fb > mejor[4]:
            mejor = (ws, "fallback", fila_inicio_hint, {"codigo":1,"stock":col_stock_hint,"precio":None,"moneda":None}, cnt_fb)

    return mejor  # puede ser (None, ... ) si todo estuvo vacío

# ========= EXTRACTORES (ID, Stock, Precio, Moneda) =========
def extraer_registros_con_stock_auto(path: Path, fila_inicio_hint: int, col_stock_hint: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws, modo, header_row, cols, filas = elegir_hoja_stock(wb, fila_inicio_hint, col_stock_hint)
        if not ws:
            log("⚠️ Hoja1:auto: no se encontró hoja con datos.")
            return out

        log(f"🧭 Hoja1:auto: usando hoja «{ws.title}» (modo={modo}, filas_detectadas={filas})")

        if modo == "encabezados":
            max_needed_col = max(v for v in cols.values() if v)
            for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                cod = row[cols["codigo"]-1] if cols["codigo"] else None
                if not _norm_text(cod): continue
                stock_raw = row[cols["stock"]-1] if cols["stock"] else None
                precio = row[cols["precio"]-1] if cols["precio"] else None
                moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(stock_raw),
                            "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        else:  # fallback
            max_row = ws.max_row or 1
            for r in range(fila_inicio_hint, max_row + 1):
                cod = ws.cell(row=r, column=1).value
                if not _norm_text(cod): continue
                raw_stock = ws.cell(row=r, column=col_stock_hint).value if col_stock_hint else None
                out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(raw_stock),
                            "Precio": None, "Moneda": None})

        if not out:
            log("⚠️ Hoja1:auto: extrajo 0 registros (revisar formato de proveedor).")
        else:
            log(f"📊 Hoja1:auto: {len(out)} registros.")
        return out
    finally:
        wb.close()

def extraer_registros_generico_xlsx(path: Path,
                                    fila_inicio_fallback: int = 2,
                                    col_precio_fb: Optional[int] = None,
                                    col_moneda_fb: Optional[int] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        # elegir hoja por encabezados para precio/moneda (o fallback simple)
        mejor = (None, None, None, {"codigo":None,"precio":None,"moneda":None}, 0)
        for ws in wb.worksheets:
            header_row, cols = detectar_columnas(ws)
            if header_row and cols["codigo"]:
                cnt = 0
                max_needed_col = max(v for v in cols.values() if v)
                for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                    cod = row[cols["codigo"]-1] if cols["codigo"] else None
                    if _norm_text(cod): cnt += 1
                if cnt > mejor[4]:
                    mejor = (ws, "encabezados", header_row, cols, cnt)

        ws, modo, header_row, cols, filas = mejor
        if ws:
            log(f"🧭 Diffs:auto: usando hoja «{ws.title}» (filas_detectadas={filas})")
            max_needed_col = max(v for v in cols.values() if v)
            for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                cod = row[cols["codigo"]-1] if cols["codigo"] else None
                if not _norm_text(cod): continue
                precio = row[cols["precio"]-1] if cols["precio"] else None
                moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                out.append({"ID": _norm_text(cod), "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        else:
            # fallback extremadamente simple (1ra col=ID)
            ws = wb.active
            for row in ws.iter_rows(min_row=fila_inicio_fallback, min_col=1, max_col=max(ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                if not _norm_text(cod): continue
                precio = row[col_precio_fb-1] if (col_precio_fb and len(row) >= col_precio_fb) else None
                moneda = row[col_moneda_fb-1] if (col_moneda_fb and len(row) >= col_moneda_fb) else None
                out.append({"ID": _norm_text(cod), "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})

        log(f"📈 Diffs:auto: {len(out)} registros.")
        return out
    finally:
        wb.close()

# Para "Hoja 1" necesitamos STOCK
def convertir_stock_generico(valor):
    t = _norm_text_lc(valor)
    if t in {"sin stock","sinstock","sin-stock","no","0","agotado","sin"}:
        return 0
    if t in {"menor a 5","<5","bajo","consultar","poco","limitado"}:
        return 2
    if t in {"mayor a 5",">5","alto","con stock","constock","en stock","stock","disponible","si","sí"}:
        return 6
    try:
        n = float(t.replace(",", ".")) if t else None
        if n is None: return None
        if n <= 0: return 0
        return 6 if n >= 5 else 2
    except Exception:
        return None

# TEVELAM (auto + espejo F/T)
def extraer_tevelam_hoja1(path: Path) -> List[Dict[str, Any]]:
    base = extraer_registros_con_stock_auto(path, fila_inicio_hint=11, col_stock_hint=9)
    out = []
    for r in base:
        out.append(r)
        s = r["ID"]
        if s and len(s) >= 2:
            if s.startswith("F"): out.append({"ID":"T"+s[1:], "Stock":r["Stock"], "Precio":r.get("Precio"), "Moneda":r.get("Moneda")})
            elif s.startswith("T"): out.append({"ID":"F"+s[1:], "Stock":r["Stock"], "Precio":r.get("Precio"), "Moneda":r.get("Moneda")})
    return out

# DISCO PRO (auto + espejo F/T)
def extraer_disco_hoja1(path: Path) -> List[Dict[str, Any]]:
    base = extraer_registros_con_stock_auto(path, fila_inicio_hint=9, col_stock_hint=7)
    out = []
    for r in base:
        out.append(r)
        s = r["ID"]
        if s and len(s) >= 2:
            if s.startswith("F"): out.append({"ID":"T"+s[1:], "Stock":r["Stock"], "Precio":r.get("Precio"), "Moneda":r.get("Moneda")})
            elif s.startswith("T"): out.append({"ID":"F"+s[1:], "Stock":r["Stock"], "Precio":r.get("Precio"), "Moneda":r.get("Moneda")})
    return out

# PROVEEDOR EXTRA (igual a tu lógica, pero si falla usamos auto)
def extraer_extra_hoja1(path: Path) -> List[Dict[str, Any]]:
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if "STOCK" in wb.sheetnames:
            st = wb["STOCK"]
            header_row, cols = detectar_columnas(st)
            out: List[Dict[str, Any]] = []
            if header_row:
                max_needed_col = max(v for v in cols.values() if v)
                for row in st.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                    cod = row[cols["codigo"]-1] if cols["codigo"] else None
                    if not _norm_text(cod): continue
                    stock_raw = row[cols["stock"]-1] if cols["stock"] else None
                    precio = row[cols["precio"]-1] if cols["precio"] else None
                    moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                    out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(stock_raw),
                                "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
                log(f"📊 EXTRA: {len(out)} regs desde hoja STOCK.")
                return out
        # fallback AUTO
        return extraer_registros_con_stock_auto(path, fila_inicio_hint=2, col_stock_hint=8)
    finally:
        wb.close()

# IMSA Hoja 1 (ya era multi-hoja)
def extraer_imsa_hoja1(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        target_ws = None
        header_row = None
        cols = {"codigo": None, "stock": None, "precio": None, "moneda": None}
        for ws in wb.worksheets:
            hr, c = detectar_columnas(ws)
            if hr:
                target_ws, header_row, cols = ws, hr, c
                break
        if not target_ws:
            for ws in wb.worksheets:
                # último recurso: fallback simple
                max_row = ws.max_row or 1
                cnt = 0
                for r in range(8, max_row+1):
                    cod = ws.cell(row=r, column=1).value
                    if _norm_text(cod): cnt += 1
                if cnt > 10:
                    target_ws = ws
                    header_row = 7
                    cols = {"codigo":1,"stock":8,"precio":None,"moneda":None}
                    break

        if not target_ws:
            log("⚠️ IMSA: no se encontró hoja con datos.")
            return out

        log(f"🧭 IMSA: usando hoja «{target_ws.title}»")

        max_needed_col = max(v for v in cols.values() if v)
        for row in target_ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
            cod = row[cols["codigo"]-1] if cols["codigo"] else None
            if not _norm_text(cod): continue
            stx = row[cols["stock"]-1] if cols["stock"] else None
            precio = row[cols["precio"]-1] if cols["precio"] else None
            moneda = row[cols["moneda"]-1] if cols["moneda"] else None
            s_cod = _norm_text(cod)
            cod_final = s_cod.split("-", 2)[-1] if s_cod.count("-") >= 2 else s_cod
            out.append({"ID": cod_final, "Stock": convertir_stock_generico(stx),
                        "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        log(f"📊 IMSA: {len(out)} registros.")
        return out
    finally:
        wb.close()

# Extractores SOLO para difs (ID, Precio, Moneda) — usan auto multi-hoja
def extraer_tevelam(path: Path) -> List[Dict[str, Any]]:
    return extraer_registros_generico_xlsx(path, fila_inicio_fallback=11)

def extraer_disco(path: Path) -> List[Dict[str, Any]]:
    return extraer_registros_generico_xlsx(path, fila_inicio_fallback=9)

def extraer_proveedor_extra(path: Path) -> List[Dict[str, Any]]:
    return extraer_registros_generico_xlsx(path, fila_inicio_fallback=2)

def extraer_imsa(path: Path) -> List[Dict[str, Any]]:
    # Igual que antes, multi-hoja para precio/moneda
    out: List[Dict[str, Any]] = []
    wb_in = load_workbook(path, read_only=True, data_only=True)
    try:
        target_ws = None
        header_row = None
        cols = {"codigo": None, "precio": None, "moneda": None}
        best = (None, None, None, None, 0)
        for ws in wb_in.worksheets:
            hr, c = detectar_columnas(ws)
            if hr and c["codigo"]:
                cnt = 0
                max_needed_col = max(v for v in c.values() if v)
                for row in ws.iter_rows(min_row=hr+1, min_col=1, max_col=max_needed_col, values_only=True):
                    cod = row[c["codigo"]-1] if c["codigo"] else None
                    if _norm_text(cod): cnt += 1
                if cnt > best[4]:
                    best = (ws, hr, c, max_needed_col, cnt)
        if best[0]:
            ws, hr, c, maxc, _ = best
            for row in ws.iter_rows(min_row=hr+1, min_col=1, max_col=maxc, values_only=True):
                cod = row[c["codigo"]-1] if c["codigo"] else None
                if not _norm_text(cod): continue
                precio = row[c["precio"]-1] if c["precio"] else None
                moneda = row[c["moneda"]-1] if c["moneda"] else None
                s_cod = _norm_text(cod)
                cod_final = s_cod.split("-", 2)[-1] if s_cod.count("-") >= 2 else s_cod
                out.append({"ID": cod_final, "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        else:
            ws = wb_in.active
            for row in ws.iter_rows(min_row=8, min_col=1, max_col=max(ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                if not _norm_text(cod): continue
                out.append({"ID": _norm_text(cod), "Precio": None, "Moneda": None})
        log(f"📈 IMSA difs: {len(out)} registros.")
        return out
    finally:
        wb_in.close()

# ========= SNAPSHOTS (ID, Precio, Moneda) =========
def _snap_path(source_key: str) -> Path:
    return SNAP_DIR / f"{source_key}_snapshot.csv"

def guardar_snapshot(source_key: str, registros: List[Dict[str, Any]]) -> None:
    p = _snap_path(source_key)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ID","Precio","Moneda"])
        for r in registros:
            w.writerow([r.get("ID"), r.get("Precio"), r.get("Moneda")])

def cargar_snapshot(source_key: str) -> Dict[str, Dict[str, Any]]:
    p = _snap_path(source_key)
    data: Dict[str, Dict[str, Any]] = {}
    if not p.exists():
        return data
    with p.open("r", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            try:
                precio_val = float(row["Precio"]) if row["Precio"] not in (None, "", "None") else None
            except Exception:
                precio_val = None
            data[row["ID"]] = {"Precio": precio_val, "Moneda": row.get("Moneda") or None}
    return data

# ========= DIFERENCIAS & REPORTE =========
def calcular_diffs(prev_snap: Dict[str, Dict[str, Any]],
                   curr_regs: List[Dict[str, Any]]
                   ) -> Tuple[List[List[Any]], List[List[Any]], List[List[Any]], List[List[Any]]]:
    curr_map: Dict[str, Dict[str, Any]] = {r["ID"]: r for r in curr_regs if r.get("ID")}
    prev_ids = set(prev_snap.keys())
    curr_ids = set(curr_map.keys())

    nuevos_ids = sorted(curr_ids - prev_ids)
    elim_ids   = sorted(prev_ids - curr_ids)
    comunes    = sorted(curr_ids & prev_ids)

    precios_up: List[List[Any]] = []
    precios_dn: List[List[Any]] = []
    nuevos: List[List[Any]] = []
    eliminados: List[List[Any]] = []

    for _id in comunes:
        prev = prev_snap.get(_id, {})
        curr = curr_map.get(_id, {})
        p_old = prev.get("Precio")
        p_new = curr.get("Precio")
        mon   = curr.get("Moneda") or prev.get("Moneda")
        if p_old is None or p_new is None:
            continue
        if p_new != p_old:
            delta = p_new - p_old
            delta_pct = (delta / p_old * 100.0) if p_old != 0 else None
            row = [_id, mon, p_old, p_new, delta, delta_pct]
            if delta > 0: precios_up.append(row)
            else:         precios_dn.append(row)

    for _id in nuevos_ids:
        c = curr_map[_id]
        nuevos.append([_id, c.get("Moneda"), c.get("Precio")])

    for _id in elim_ids:
        p = prev_snap[_id]
        eliminados.append([_id, p.get("Moneda"), p.get("Precio")])

    return precios_up, precios_dn, nuevos, eliminados

def hay_cambios(precios_up, precios_dn, nuevos, eliminados) -> bool:
    return any([precios_up, precios_dn, nuevos, eliminados])

def crear_libro_cambios(source_key: str,
                        precios_up: List[List[Any]],
                        precios_dn: List[List[Any]],
                        nuevos: List[List[Any]],
                        eliminados: List[List[Any]]) -> Path:
    wb = Workbook(write_only=True)
    sh_res = wb.create_sheet("Resumen")
    sh_up  = wb.create_sheet("Precios ↑")
    sh_dn  = wb.create_sheet("Precios ↓")
    sh_new = wb.create_sheet("Nuevos modelos")
    sh_del = wb.create_sheet("Modelos eliminados")
    try:
        d = wb.worksheets[0]
        if d.title not in {"Resumen","Precios ↑","Precios ↓","Nuevos modelos","Modelos eliminados"}:
            wb.remove(d)
    except Exception:
        pass

    sh_up.append(["ID","Moneda","Precio anterior","Precio nuevo","Δ","Δ %"])
    sh_dn.append(["ID","Moneda","Precio anterior","Precio nuevo","Δ","Δ %"])
    sh_new.append(["ID","Moneda","Precio"])
    sh_del.append(["ID","Moneda","Precio"])

    for r in precios_up: sh_up.append(r)
    for r in precios_dn: sh_dn.append(r)
    for r in nuevos:     sh_new.append(r)
    for r in eliminados: sh_del.append(r)

    cnt_up = len(precios_up)
    cnt_dn = len(precios_dn)
    cnt_new = len(nuevos)
    cnt_del = len(eliminados)
    sum_up = round(sum((x[4] for x in precios_up)), 4) if cnt_up else 0
    sum_dn = round(sum((x[4] for x in precios_dn)), 4) if cnt_dn else 0

    sh_res.append(["Fuente", source_key])
    sh_res.append(["Generado", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    sh_res.append([])
    sh_res.append(["Métrica","Valor"])
    sh_res.append(["Precios ↑ (cantidad)", cnt_up])
    sh_res.append(["Precios ↓ (cantidad)", cnt_dn])
    sh_res.append(["Suma Δ ↑", sum_up])
    sh_res.append(["Suma Δ ↓", sum_dn])
    sh_res.append(["Nuevos modelos", cnt_new])
    sh_res.append(["Modelos eliminados", cnt_del])

    out = REPORTS_DIR / f"{source_key}_DIFF_{ts()}.xlsx"
    wb.save(out)
    log(f"🧾 Reporte generado: {out.name}")

    # copia pública
    try:
        (PUBLIC_REPORTS_DIR / out.name).write_bytes(out.read_bytes())
    except Exception as e:
        log(f"⚠️ No se pudo copiar reporte a public_reports: {e}")

    return out

# ========= SALIDA “Hoja 1” =========
def guardar_hoja1_xlsx(source_key: str, path_base: Path, registros: List[Dict[str, Any]], nombre_salida: Optional[str] = None) -> Path:
    wb_out = Workbook(write_only=True)
    h1 = wb_out.create_sheet("Hoja 1")
    try:
        d = wb_out.worksheets[0]
        if d.title != "Hoja 1":
            wb_out.remove(d)
    except Exception:
        pass
    h1.append(["ID","Stock","Precio","Moneda"])
    for r in registros:
        h1.append([r.get("ID"), r.get("Stock"), r.get("Precio"), r.get("Moneda")])
    out = path_base.with_name(path_base.stem + (nombre_salida or "_HOJA1") + ".xlsx")
    wb_out.save(out)
    log(f"✅ {path_base.stem} → {out.name} (rows={len(registros)})")

    try:
        safe = f"{source_key}_ULTIMA.xlsx"
        (PUBLIC_LISTAS_DIR / safe).write_bytes(out.read_bytes())
        log(f"📤 Publicada Hoja 1 → public_listas/{safe}")
    except Exception as e:
        log(f"⚠️ No se pudo copiar Hoja 1 a public_listas: {e}")

    return out

# ========= REFRESCO DE “Hoja 1” DESDE BASE =========
def ensure_hoja1_desde_base(source_key: str, extractor_hoja1) -> None:
    dbp = _db_path(source_key)
    if not dbp.exists():
        log(f"ℹ️ {source_key}: no hay base en _db para refrescar Hoja 1.")
        return
    try:
        regs_h1 = extractor_hoja1(dbp)
        guardar_hoja1_xlsx(source_key, dbp, regs_h1)
        log(f"🔁 {source_key}: Hoja 1 refrescada desde base.")
    except Exception as e:
        log(f"⚠️ {source_key}: error refrescando Hoja 1 desde base: {e}")

# ========= SELENIUM (headless) =========
def _build_chrome() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument("--window-size=1920,1080")
    prefs = {"download.default_directory": str(RUTA_DESCARGA),"download.prompt_for_download": False,"download.directory_upgrade": True,"safebrowsing.enabled": True}
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def _close_driver(driver: webdriver.Chrome):
    try: driver.quit()
    except Exception: pass

def _find_recent_listaimsa(max_age_sec: int = 180) -> Optional[Path]:
    now = time.time()
    pats = ("lista", "imsa")
    exts = (".xlsx", ".xls")
    candidatos = []
    for p in RUTA_DESCARGA.iterdir():
        if not p.is_file(): continue
        if p.suffix.lower() not in exts: continue
        name = p.name.lower()
        if not any(s in name for s in pats): continue
        try: mtime = p.stat().st_mtime
        except Exception: continue
        if now - mtime <= max_age_sec:
            if (RUTA_DESCARGA / (p.name + ".crdownload")).exists():
                continue
            candidatos.append(p)
    if not candidatos: return None
    return max(candidatos, key=lambda x: x.stat().st_mtime)

def descargar_imsa_web() -> Optional[Path]:
    driver = None
    try:
        driver = _build_chrome()
        log("🌐 Abriendo IMSA…")
        driver.get(IMSA_URL)
        try:
            iframes = WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.TAG_NAME, "iframe")))
            driver.switch_to.frame(iframes[0])
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "pass"))).send_keys(IMSA_PASSWORD)
            btn = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Login']")
            driver.execute_script("arguments[0].click();", btn)
            log("🔑 Login enviado.")
        except Exception as e:
            log(f"⚠️ No se pudo automatizar el login: {e}")
        finally:
            try: driver.switch_to.default_content()
            except Exception: pass

        log("⏳ Esperando 60s para descarga automática…")
        time.sleep(60)

        cand = _find_recent_listaimsa(max_age_sec=240)
        if cand:
            log(f"✅ IMSA detectado: {cand.name}")
            return cand

        log("🔎 Polling carpeta (hasta 60s)…")
        deadline = time.time() + 60
        while time.time() < deadline:
            cand = _find_recent_listaimsa(max_age_sec=240)
            if cand:
                log(f"✅ IMSA detectado: {cand.name}")
                return cand
            time.sleep(2)
        log("⚠️ No se detectó archivo IMSA.")
        return None
    except Exception as e:
        log(f"❌ Error en flujo IMSA: {e}")
        return None
    finally:
        if driver:
            _close_driver(driver)
            log("🧹 Selenium cerrado.")

# ========= MAIN =========
if __name__ == "__main__":
    log("INICIO — HASH completo + BASE visible + GATE diario + HOJA1 multi-hoja + DIFERENCIAS + REFRESCO DESDE BASE")

    # 1) DESCARGAS (con gate diario por fuente)
    tevelam = None
    if not skip_por_base_de_hoy("Tevelam"):
        try:
            log("Descargando Tevelam…")
            tevelam = download_simple(URL_TEVELAM, "Tevelam")
            log(f"Tevelam → {tevelam}")
        except Exception as e:
            log(f"ERROR Tevelam: {e}")

    disco = None
    if not skip_por_base_de_hoy("Disco_Pro"):
        try:
            log("Descargando Disco Pro…")
            disco = download_simple(URL_DISCO_PRO, "Disco_Pro")
            log(f"Disco_Pro → {disco}")
        except Exception as e:
            log(f"ERROR Disco Pro: {e}")

    extra = None
    if not skip_por_base_de_hoy("ARS_Tech"):
        try:
            log("Descargando Proveedor Extra…")
            extra = download_simple(URL_PROVEEDOR_EXTRA, "ARS_Tech")
            log(f"ARS_Tech → {extra}")
        except Exception as e:
            log(f"ERROR Proveedor Extra: {e}")

    imsa = None
    if not skip_por_base_de_hoy("IMSA"):
        try:
            log("Lanzando IMSA…")
            imsa = descargar_imsa_web()
            log(f"IMSA detectado → {imsa}")
        except Exception as e:
            log(f"ERROR flujo IMSA: {e}")

    # 2) HASH & PROCESO
    log("Comparando hashes y generando salidas…")
    omitidos = []
    procesados = set()

    def guardar_hoja1(source_key: str, path: Path, registros: List[Dict[str, Any]]):
        guardar_hoja1_xlsx(source_key, path, registros)

    def run_fuente(source_key: str, path: Optional[Path],
                   extractor_diffs, extractor_hoja1):
        if not path:
            return
        if decide_should_process(source_key, path):
            # A) HOJA 1
            try:
                regs_h1 = extractor_hoja1(path)
                guardar_hoja1(source_key, path, regs_h1)
            except Exception as e:
                log(f"⚠️ {source_key}: error generando Hoja 1: {e}")

            # B) DIFERENCIAS (precios/modelos)
            try:
                regs = extractor_diffs(path)
                prev = cargar_snapshot(source_key)
                precios_up, precios_dn, nuevos, eliminados = calcular_diffs(prev, regs)
                if hay_cambios(precios_up, precios_dn, nuevos, eliminados):
                    _ = crear_libro_cambios(source_key, precios_up, precios_dn, nuevos, eliminados)
                else:
                    log(f"ℹ️ {source_key}: hubo hash nuevo pero sin cambios de precio/modelos → no se genera libro.")
                guardar_snapshot(source_key, regs)
            except Exception as e:
                log(f"⚠️ {source_key}: error calculando/generando diffs: {e}")
            procesados.add(source_key)
        else:
            omitidos.append(source_key)

    if tevelam: run_fuente("Tevelam", tevelam, extraer_tevelam,        extraer_tevelam_hoja1)
    if disco:   run_fuente("Disco_Pro", disco, extraer_disco,          extraer_disco_hoja1)
    if extra:   run_fuente("ARS_Tech", extra, extraer_proveedor_extra, extraer_extra_hoja1)
    if imsa:    run_fuente("IMSA", imsa,    extraer_imsa,              extraer_imsa_hoja1)

    # 2.b) REFRESCAR SIEMPRE “Hoja 1” DESDE LA BASE (por si no hubo cambios / gate diario)
    ensure_hoja1_desde_base("Tevelam",   extraer_tevelam_hoja1)   if "Tevelam"   not in procesados else None
    ensure_hoja1_desde_base("Disco_Pro", extraer_disco_hoja1)     if "Disco_Pro" not in procesados else None
    ensure_hoja1_desde_base("ARS_Tech",  extraer_extra_hoja1)     if "ARS_Tech"  not in procesados else None
    ensure_hoja1_desde_base("IMSA",      extraer_imsa_hoja1)      if "IMSA"      not in procesados else None

    # 3) RESUMEN
    log("================ RESUMEN ================")
    if omitidos:
        log("Omitidos por hash igual/gate (Hoja 1 se refrescó desde base):")
        for n in omitidos:
            log(f"  • {n}")
    else:
        log("No hubo fuentes omitidas por hash igual/gate.")
    log(f"FIN en: {RUTA_DESCARGA}")
