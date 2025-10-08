# hash_comparativo.py — descarga + SHA completo + DB pública + Stock V + difs (estado persistente)

from __future__ import annotations

import csv
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import os

from zoneinfo import ZoneInfo

import requests
from openpyxl import load_workbook, Workbook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ========= CONFIG =========
BASE_DIR = Path(os.getenv("WORKDIR", ".")).resolve()

# Descargas efímeras
RUTA_DESCARGA = BASE_DIR / "downloads"

# ESTADO PERSISTENTE (SE COMITEA): hashes y snapshots
STATE_DIR       = BASE_DIR / "_db"
STATE_HASH_DIR  = STATE_DIR / "hash"
STATE_SNAP_DIR  = STATE_DIR / "snapshots"

# Publicación (GitHub Pages)
PUBLIC_DB_DIR      = BASE_DIR / "public_db"       # copia exacta (siempre se sobreescribe el último)
PUBLIC_LISTAS_DIR  = BASE_DIR / "public_listas"   # Stock V (con timestamp en el nombre)
PUBLIC_REPORTS_DIR = BASE_DIR / "public_reports"  # reportes de difs

# (opcionales efímeras locales)
REPORTS_DIR = BASE_DIR / "_reports"
SNAP_DIR    = BASE_DIR / "_snapshots"
HASH_DB_DIR = BASE_DIR / "_hashdb"  # compat (no obligatorio)

for d in (
    RUTA_DESCARGA, STATE_DIR, STATE_HASH_DIR, STATE_SNAP_DIR,
    PUBLIC_DB_DIR, PUBLIC_LISTAS_DIR, PUBLIC_REPORTS_DIR,
    REPORTS_DIR, SNAP_DIR
):
    d.mkdir(parents=True, exist_ok=True)

# Si el hash es igual → borrar descarga
BORRAR_DUPLICADO = os.getenv("BORRAR_DUPLICADO", "true").lower() == "true"

# Banderas IMSA
IMSA_SOLO_CON_STOCK = os.getenv("IMSA_SOLO_CON_STOCK", "false").lower() == "true"

# URLs fuentes (ejemplo; reemplazá si cambia la fuente)
URL_TEVELAM = "https://drive.google.com/uc?export=download&id=1hPH3VwQDtMgx_AkC5hFCUbM2MEiwBEpT"
URL_DISCO_PRO = "https://drive.google.com/uc?id=1-aQ842Dq3T1doA-Enb34iNNzenLGkVkr&export=download"
URL_PROVEEDOR_EXTRA = "https://docs.google.com/uc?id=1JnUnrpZUniTXUafkAxCInPG7O39yrld5&export=download"

# IMSA (iframe con password)
IMSA_URL = "https://listaimsa.com.ar/lista-de-precios/"
IMSA_PASSWORD = os.getenv("IMSA_PASSWORD", "lista2021")

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
TIMEOUT = 60

# ========= LOG / UTILS =========
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ========= HASH =========
def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

def _hash_path(source_key: str) -> Path:
    # hash PERSISTENTE
    return STATE_HASH_DIR / f"{source_key}.sha256"

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

def decide_should_process(source_key: str, path: Optional[Path]) -> Tuple[bool, Optional[str]]:
    """Devuelve (procesar?, new_hash). No procesa si el hash es igual al previo."""
    if not path or not path.exists():
        log(f"⏭️ {source_key}: no hay archivo para comparar.")
        return False, None
    try:
        new_hash = file_sha256(path)
        prev_hash = read_prev_hash(source_key)
        log(f"📇 {source_key}: nuevo={new_hash[:12]}… | previo={(prev_hash[:12] + '…') if prev_hash else 'N/A'}")
        if prev_hash == new_hash:
            log(f"⏭️ {source_key}: sin cambios (hash igual).")
            if BORRAR_DUPLICADO:
                try:
                    path.unlink(missing_ok=True)
                    log(f"🗑️ {source_key}: duplicado eliminado: {path.name}")
                except Exception as e:
                    log(f"⚠️ {source_key}: no se pudo borrar duplicado: {e}")
            return False, new_hash
        write_hash(source_key, new_hash)
        log(f"🔄 {source_key}: cambios detectados (hash distinto) → conservo y proceso.")
        return True, new_hash
    except Exception as e:
        log(f"⚠️ {source_key}: error comparando hash: {e} → por las dudas conservo y proceso.")
        return True, None

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

# candidatos de encabezados
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

# ========= STOCK (mapeo genérico) =========
def convertir_stock_generico(valor):
    t = _norm_text_lc(valor)
    if t in {"sin stock","sinstock","sin-stock","no","0","agotado","sin"}:
        return 0
    if t in {"menor a 5","<5","bajo","consultar","poco","limitado"}:
        return 2
    if t in {"mayor a 5",">5","alto","con stock","constock","en stock","stock","disponible","si","sí"}:
        return 6
    try:
        n = float(t.replace(",", "."))
        if n <= 0: return 0
        return 6 if n >= 5 else 2
    except Exception:
        return None

def extraer_registros_con_stock_fallback(path: Path, fila_inicio: int, col_stock: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        header_row, cols = detectar_columnas(ws)
        if header_row and cols["codigo"]:
            max_needed_col = max(v for v in cols.values() if v)
            for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                cod = row[cols["codigo"]-1] if cols["codigo"] else None
                if not _norm_text(cod): continue
                stock_raw = row[cols["stock"]-1] if cols["stock"] else None
                precio = row[cols["precio"]-1] if cols["precio"] else None
                moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(stock_raw),
                            "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        else:
            max_row = ws.max_row or 1
            for r in range(fila_inicio, max_row + 1):
                cod = ws.cell(row=r, column=1).value
                if not _norm_text(cod): continue
                raw_stock = ws.cell(row=r, column=col_stock).value
                out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(raw_stock),
                            "Precio": None, "Moneda": None})
    finally:
        wb.close()
    return out

def _filter_stock_positive(regs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in regs:
        st = r.get("Stock")
        try:
            keep = (st is not None) and (float(st) > 0)
        except Exception:
            keep = False
        if keep:
            out.append(r)
    return out

# ========= EXTRACTORES Hoja 1 =========
# TEVELAM Hoja1 (inicio 11, stock col 9) + espejo F/T
def extraer_tevelam_hoja1(path: Path) -> List[Dict[str, Any]]:
    regs = extraer_registros_con_stock_fallback(path, fila_inicio=11, col_stock=9)
    regs = _filter_stock_positive(regs)
    out = []
    for r in regs:
        out.append(r)
        s = r["ID"]
        if len(s) >= 2:
            if s.startswith("F"):
                out.append({"ID": "T"+s[1:], "Stock": r["Stock"], "Precio": r.get("Precio"), "Moneda": r.get("Moneda")})
            elif s.startswith("T"):
                out.append({"ID": "F"+s[1:], "Stock": r["Stock"], "Precio": r.get("Precio"), "Moneda": r.get("Moneda")})
    return out

# DISCO PRO Hoja1 (inicio 9, stock col 7) + espejo F/T
def extraer_disco_hoja1(path: Path) -> List[Dict[str, Any]]:
    regs = extraer_registros_con_stock_fallback(path, fila_inicio=9, col_stock=7)
    regs = _filter_stock_positive(regs)
    out = []
    for r in regs:
        out.append(r)
        s = r["ID"]
        if len(s) >= 2:
            if s.startswith("F"):
                out.append({"ID": "T"+s[1:], "Stock": r["Stock"], "Precio": r.get("Precio"), "Moneda": r.get("Moneda")})
            elif s.startswith("T"):
                out.append({"ID": "F"+s[1:], "Stock": r["Stock"], "Precio": r.get("Precio"), "Moneda": r.get("Moneda")})
    return out

# ARS_Tech / Proveedor extra Hoja1
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
            else:
                out = []
                max_row = st.max_row or 1
                for r in range(2, max_row + 1):
                    _id = st.cell(row=r, column=2).value
                    if not _norm_text(_id): continue
                    raw = st.cell(row=r, column=4).value
                    out.append({"ID": _norm_text(_id), "Stock": convertir_stock_generico(raw),
                                "Precio": None, "Moneda": None})
        else:
            out = extraer_registros_con_stock_fallback(path, fila_inicio=2, col_stock=8)
    finally:
        wb.close()
    return _filter_stock_positive(out)

# IMSA Hoja1 (con opción "sólo 'con stock'")
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
            # fallback básico
            target_ws = wb.active
            for row in target_ws.iter_rows(min_row=8, min_col=1, max_col=max(target_ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                stx = row[7] if len(row) >= 8 else None
                if not _norm_text(cod): continue
                if IMSA_SOLO_CON_STOCK:
                    if _norm_text_lc(stx) != "con stock":
                        continue
                    stock_val = 6
                else:
                    stock_val = convertir_stock_generico(stx)
                out.append({"ID": _norm_text(cod), "Stock": stock_val, "Precio": None, "Moneda": None})
            return out

        max_needed_col = max(v for v in cols.values() if v)
        for row in target_ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
            cod = row[cols["codigo"]-1] if cols["codigo"] else None
            if not _norm_text(cod): continue
            stx = row[cols["stock"]-1] if cols["stock"] else None
            precio = row[cols["precio"]-1] if cols["precio"] else None
            moneda = row[cols["moneda"]-1] if cols["moneda"] else None
            s_cod = _norm_text(cod)
            cod_final = s_cod.split("-", 2)[-1] if s_cod.count("-") >= 2 else s_cod

            if IMSA_SOLO_CON_STOCK:
                # requisito: SOLO leyenda exacta "con stock"
                if _norm_text_lc(stx) != "con stock":
                    continue
                stock_val = 6
            else:
                stock_val = convertir_stock_generico(stx)

            out.append({"ID": cod_final, "Stock": stock_val,
                        "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
    finally:
        wb.close()
    return out

# ========= EXTRACTORES SOLO difs (ID, Precio, Moneda) =========
def extraer_registros_generico_xlsx(path: Path,
                                    fila_inicio_fallback: int = 2,
                                    col_precio_fb: Optional[int] = None,
                                    col_moneda_fb: Optional[int] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        header_row, cols = detectar_columnas(ws)
        if header_row:
            max_needed_col = max(v for v in cols.values() if v)
            for row in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
                cod = row[cols["codigo"]-1] if cols["codigo"] else None
                if not _norm_text(cod): continue
                precio = row[cols["precio"]-1] if cols["precio"] else None
                moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                out.append({"ID": _norm_text(cod), "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
        else:
            for row in ws.iter_rows(min_row=fila_inicio_fallback, min_col=1, max_col=max(ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                if not _norm_text(cod): continue
                precio = row[col_precio_fb-1] if (col_precio_fb and len(row) >= col_precio_fb) else None
                moneda = row[col_moneda_fb-1] if (col_moneda_fb and len(row) >= col_moneda_fb) else None
                out.append({"ID": _norm_text(cod), "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
    finally:
        wb.close()
    return out

def extraer_tevelam(path: Path) -> List[Dict[str, Any]]:
    return extraer_registros_generico_xlsx(path, fila_inicio_fallback=11)

def extraer_disco(path: Path) -> List[Dict[str, Any]]:
    return extraer_registros_generico_xlsx(path, fila_inicio_fallback=9)

def extraer_proveedor_extra(path: Path) -> List[Dict[str, Any]]:
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
                    precio = row[cols["precio"]-1] if cols["precio"] else None
                    moneda = row[cols["moneda"]-1] if cols["moneda"] else None
                    out.append({"ID": _norm_text(cod), "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
                return out
        return extraer_registros_generico_xlsx(path, fila_inicio_fallback=2)
    finally:
        wb.close()

def extraer_imsa(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    wb_in = load_workbook(path, read_only=True, data_only=True)
    try:
        target_ws = None
        header_row = None
        cols = {"codigo": None, "precio": None, "moneda": None}
        for ws in wb_in.worksheets:
            hr, c = detectar_columnas(ws)
            if hr:
                target_ws, header_row, cols = ws, hr, c
                break
        if not target_ws:
            target_ws = wb_in.active
            for row in target_ws.iter_rows(min_row=8, min_col=1, max_col=max(target_ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                if not _norm_text(cod): continue
                out.append({"ID": _norm_text(cod), "Precio": None, "Moneda": None})
            return out

        max_needed_col = max(v for v in cols.values() if v)
        for row in target_ws.iter_rows(min_row=header_row+1, min_col=1, max_col=max_needed_col, values_only=True):
            cod = row[cols["codigo"]-1] if cols["codigo"] else None
            if not _norm_text(cod): continue
            precio = row[cols["precio"]-1] if cols["precio"] else None
            moneda = row[cols["moneda"]-1] if cols["moneda"] else None
            s_cod = _norm_text(cod)
            cod_final = s_cod.split("-", 2)[-1] if s_cod.count("-") >= 2 else s_cod
            out.append({"ID": cod_final, "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
    finally:
        wb_in.close()
    return out

# ========= SNAPSHOTS & REPORTES =========
def _snap_path(source_key: str) -> Path:
    # snapshot PERSISTENTE
    return STATE_SNAP_DIR / f"{source_key}_snapshot.csv"

def guardar_snapshot(source_key: str, registros: List[Dict[str, Any]]) -> None:
    p = _snap_path(source_key)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ID","Precio","Moneda"])
        for r in registros:
            w.writerow([r.get("ID"), r.get("Precio"), r.get("Moneda")])
    # copia efímera opcional
    try:
        (SNAP_DIR / p.name).write_bytes(p.read_bytes())
    except Exception:
        pass

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

    sh_up.append(["
