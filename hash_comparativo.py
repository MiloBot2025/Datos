# hash_comparativo.py — Descargas, Hoja 1 y reporte de cambios (baseline diaria + cooldown)

import csv
import os
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import requests
from openpyxl import load_workbook, Workbook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ========= CONFIG =========
# WORKDIR: si existe variable de entorno (GitHub Actions), úsala; si no, la carpeta Descargas local.
RUTA_DESCARGA = Path(os.environ.get("WORKDIR", str(Path.home() / "Downloads"))).resolve()
RUTA_DESCARGA.mkdir(parents=True, exist_ok=True)

# directorios (algunos históricos para compatibilidad; los de hash/snap ya no se usan)
HASH_DB_DIR = RUTA_DESCARGA / "_hashdb"
HASH_DB_DIR.mkdir(parents=True, exist_ok=True)
SNAP_DIR = RUTA_DESCARGA / "_snapshots"
SNAP_DIR.mkdir(parents=True, exist_ok=True)

REPORTS_DIR = RUTA_DESCARGA / "_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

PUBLIC_LISTAS_DIR = RUTA_DESCARGA / "public_listas"
PUBLIC_LISTAS_DIR.mkdir(parents=True, exist_ok=True)

# Si el hash es igual al previo → se elimina el archivo recién bajado. (YA NO lo usamos)
# Por la nueva política pediste **conservar siempre el original**:
BORRAR_DUPLICADO = False

# URLs de proveedores
URL_TEVELAM = "https://drive.google.com/uc?export=download&id=1hPH3VwQDtMgx_AkC5hFCUbM2MEiwBEpT"
URL_DISCO_PRO = "https://drive.google.com/uc?id=1-aQ842Dq3T1doA-Enb34iNNzenLGkVkr&export=download"
URL_PROVEEDOR_EXTRA = "https://docs.google.com/uc?id=1JnUnrpZUniTXUafkAxCInPG7O39yrld5&export=download"

IMSA_URL = "https://listaimsa.com.ar/lista-de-precios/"
# lee la contraseña desde el entorno si está (en Actions usamos un secret), si no, usa el valor por defecto
IMSA_PASSWORD = os.environ.get("IMSA_PASSWORD", "lista2021")

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
TIMEOUT = 60

# IMSA: incluir TODO para analizar precio aunque no haya stock
IMSA_SOLO_CON_STOCK = False
IMSA_BORRAR_ORIGINAL = False  # nunca borrar el original tras procesar

# ========= LOG / UTILS =========
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ========= BASELINE DIARIA / COOLDOWN (sin DB histórica) =========
def _today_key() -> str:
    # El runner usa TZ configurada por el workflow; localmente usa la del SO
    return datetime.now().strftime("%Y%m%d")

DAILY_DIR = RUTA_DESCARGA / "_daily"   # p.ej. run_out/_daily/20251004/…
DAILY_DIR.mkdir(parents=True, exist_ok=True)

def daily_paths(source_key: str) -> Tuple[Path, Path, Path]:
    """
    baseline.csv y .lock por proveedor y día.
    Además, una carpeta por día para guardar originales.
    """
    day_dir = DAILY_DIR / _today_key()
    day_dir.mkdir(parents=True, exist_ok=True)
    originals_dir = day_dir / "_originals"
    originals_dir.mkdir(parents=True, exist_ok=True)
    baseline = day_dir / f"{source_key}_baseline.csv"
    lock = day_dir / f"{source_key}.lock"
    return baseline, lock, originals_dir

def write_baseline_csv(baseline: Path, registros: List[Dict[str, Any]]) -> None:
    with baseline.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ID","Precio","Moneda"])
        for r in registros:
            w.writerow([r.get("ID"), r.get("Precio"), r.get("Moneda")])

def read_baseline_csv(baseline: Path) -> Dict[str, Dict[str, Any]]:
    if not baseline.exists():
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with baseline.open("r", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            try:
                p = float(row["Precio"]) if row["Precio"] not in (None, "", "None") else None
            except Exception:
                p = None
            out[row["ID"]] = {"Precio": p, "Moneda": row.get("Moneda") or None}
    return out

# ========= HASH (quedan helpers por compatibilidad/debug; no definen el flujo) =========
def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)
    return h.hexdigest()

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

# ========= EXTRACTORES (ID, Precio, Moneda) =========
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
        # Intento por encabezados primero
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
            # Fallback histórico (como “antes”)
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

# TEVELAM (Hoja 1: inicio 11, stock col I=9) + espejo F/T
def extraer_tevelam_hoja1(path: Path) -> List[Dict[str, Any]]:
    regs = extraer_registros_con_stock_fallback(path, fila_inicio=11, col_stock=9)
    out = []
    for r in regs:
        out.append(r)
        s = r["ID"]
        if len(s) >= 2:
            if s.startswith("F"):
                out.append({"ID": "T"+s[1:], "Stock": r["Stock"], "Precio": None, "Moneda": None})
            elif s.startswith("T"):
                out.append({"ID": "F"+s[1:], "Stock": r["Stock"], "Precio": None, "Moneda": None})
    return out

# DISCO PRO (Hoja 1: inicio 9, stock col G=7) + espejo F/T
def extraer_disco_hoja1(path: Path) -> List[Dict[str, Any]]:
    regs = extraer_registros_con_stock_fallback(path, fila_inicio=9, col_stock=7)
    out = []
    for r in regs:
        out.append(r)
        s = r["ID"]
        if len(s) >= 2:
            if s.startswith("F"):
                out.append({"ID": "T"+s[1:], "Stock": r["Stock"], "Precio": None, "Moneda": None})
            elif s.startswith("T"):
                out.append({"ID": "F"+s[1:], "Stock": r["Stock"], "Precio": None, "Moneda": None})
    return out

# PROVEEDOR EXTRA (Hoja 1: hoja STOCK o fallback B/D)
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
                return out
            else:
                out = []
                max_row = st.max_row or 1
                for r in range(2, max_row + 1):
                    _id = st.cell(row=r, column=2).value
                    if not _norm_text(_id): continue
                    raw = st.cell(row=r, column=4).value
                    out.append({"ID": _norm_text(_id), "Stock": convertir_stock_generico(raw),
                                "Precio": None, "Moneda": None})
                return out
        else:
            return extraer_registros_con_stock_fallback(path, fila_inicio=2, col_stock=8)
    finally:
        wb.close()

# IMSA Hoja 1
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
            target_ws = wb.active
            for row in target_ws.iter_rows(min_row=8, min_col=1, max_col=max(target_ws.max_column, 1), values_only=True):
                cod = row[0] if len(row) >= 1 else None
                stx = row[7] if len(row) >= 8 else None
                if not _norm_text(cod): continue
                out.append({"ID": _norm_text(cod), "Stock": convertir_stock_generico(stx),
                            "Precio": None, "Moneda": None})
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
            out.append({"ID": cod_final, "Stock": convertir_stock_generico(stx),
                        "Precio": try_float(precio), "Moneda": _norm_text(moneda) or None})
    finally:
        wb.close()
    return out

# Extractores SOLO para difs (ID, Precio, Moneda)
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

# ========= SNAPSHOTS (histórico) — ya no se usan, pero dejamos helpers si hiciera falta =========
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
    return out

# ========= SALIDA “Hoja 1” =========
def guardar_hoja1_xlsx(path_base: Path, registros: List[Dict[str, Any]], nombre_salida: Optional[str] = None) -> Path:
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
    out = (PUBLIC_LISTAS_DIR / (path_base.stem + (nombre_salida or "_HOJA1") + ".xlsx"))
    wb_out.save(out)
    log(f"✅ {path_base.stem} → {out.name}")
    return out

# ========= IMSA (Selenium) =========
def _build_chrome() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument("--start-maximized")
    # en CI
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": str(RUTA_DESCARGA),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service()  # Selenium Manager resuelve el driver
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
    if not candidatos:
        return None
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

# ========= MAIN (con baseline diaria + cooldown) =========
if __name__ == "__main__":
    log("INICIO (baseline diaria + cooldown por proveedor)")

    # 1) DESCARGAS
    try:
        log("Descargando Tevelam…")
        tevelam = download_simple(URL_TEVELAM, "Tevelam")
        log(f"Tevelam → {tevelam}")
    except Exception as e:
        log(f"ERROR Tevelam: {e}"); tevelam = None

    try:
        log("Descargando Disco Pro…")
        disco = download_simple(URL_DISCO_PRO, "Disco_Pro")
        log(f"Disco_Pro → {disco}")
    except Exception as e:
        log(f"ERROR Disco Pro: {e}"); disco = None

    try:
        log("Descargando Proveedor Extra…")
        extra = download_simple(URL_PROVEEDOR_EXTRA, "ARS_Tech")
        log(f"ARS_Tech → {extra}")
    except Exception as e:
        log(f"ERROR Proveedor Extra: {e}"); extra = None

    try:
        log("Lanzando IMSA…")
        imsa = descargar_imsa_web()
        log(f"IMSA detectado → {imsa}")
    except Exception as e:
        log(f"ERROR flujo IMSA: {e}"); imsa = None

    # 2) PROCESO (Hoja1 + diffs vs baseline diaria + cooldown)
    log("Procesando y generando salidas…")
    omitidos: List[str] = []

    def guardar_hoja1(path: Path, registros: List[Dict[str, Any]]):
        guardar_hoja1_xlsx(path, registros)

    def run_fuente(source_key: str, path: Optional[Path],
                   extractor_diffs, extractor_hoja1):
        if not path:
            return

        baseline_path, lock_path, originals_dir = daily_paths(source_key)

        # Siempre conservar el original también dentro del día (para auditoría rápida)
        try:
            if path and path.exists():
                target_copy = originals_dir / path.name
                if not target_copy.exists():
                    target_copy.write_bytes(path.read_bytes())
        except Exception as e:
            log(f"⚠️ {source_key}: no se pudo copiar original del día: {e}")

        # Cooldown diario: si ya hubo cambios hoy, no procesamos más
        if lock_path.exists():
            log(f"⏭️ {source_key}: cooldown activo (ya hubo cambios hoy) → omito hasta mañana 07:00.")
            omitidos.append(source_key)
            return

        # A) HOJA 1 (siempre)
        try:
            regs_h1 = extractor_hoja1(path)
            guardar_hoja1(path, regs_h1)
        except Exception as e:
            log(f"⚠️ {source_key}: error generando Hoja 1: {e}")

        # B) Diffs contra baseline diaria
        try:
            curr_regs = extractor_diffs(path)

            if not baseline_path.exists():
                write_baseline_csv(baseline_path, curr_regs)
                log(f"📌 {source_key}: baseline diaria creada ({baseline_path.name}). No hay reporte en esta primera pasada.")
                return

            prev_snap = read_baseline_csv(baseline_path)
            precios_up, precios_dn, nuevos, eliminados = calcular_diffs(prev_snap, curr_regs)

            if hay_cambios(precios_up, precios_dn, nuevos, eliminados):
                _ = crear_libro_cambios(source_key, precios_up, precios_dn, nuevos, eliminados)
                # marcamos cooldown para no volver a procesar hoy
                lock_path.touch()
                # actualizamos la baseline a la última versión del día (opcional)
                write_baseline_csv(baseline_path, curr_regs)
                # bandera para email/web si querés usarla en el workflow
                try:
                    (RUTA_DESCARGA / "CHANGES_FLAG").write_text("1", encoding="utf-8")
                except Exception:
                    pass
                log(f"✅ {source_key}: cambios detectados → cooldown hasta mañana 07:00.")
            else:
                log(f"ℹ️ {source_key}: sin cambios vs baseline de hoy → no se genera libro.")
        except Exception as e:
            log(f"⚠️ {source_key}: error calculando/generando diffs: {e}")

    if tevelam: run_fuente("Tevelam",   tevelam, extraer_tevelam,        extraer_tevelam_hoja1)
    if disco:   run_fuente("Disco_Pro", disco,   extraer_disco,          extraer_disco_hoja1)
    if extra:   run_fuente("ARS_Tech",  extra,   extraer_proveedor_extra, extraer_extra_hoja1)
    if imsa:    run_fuente("IMSA",      imsa,    extraer_imsa,            extraer_imsa_hoja1)

    # 3) RESUMEN
    log("================ RESUMEN ================")
    if omitidos:
        log("Omitidos por cooldown activo (ya hubo cambios hoy):")
        for n in omitidos:
            log(f"  • {n}")
    else:
        log("No hubo fuentes omitidas por cooldown.")

    log(f"FIN en: {RUTA_DESCARGA}")
