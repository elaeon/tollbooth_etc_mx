---
description: Extrae tarifas de autopistas de un sitio web operador y guarda como CSV
argument-hint: <URL> [archivo_salida.csv]
allowed-tools: Bash(playwright-cli:*), Bash(uv run python:*), Bash(mkdir:*), Bash(python3:*), Read, Write, Bash(uv run markitdown), Bash(ls:*), Bash(head:*)
---

# Scrape Tarifas

Extrae tarifas de peaje de un sitio web operador de autopistas mexicanas y las guarda en CSV.

## Argumentos

`$ARGUMENTS` puede contener:
- URL del sitio operador (por defecto: `https://orler.com.mx/`)
- Opcionalmente, ruta del CSV de salida separada por espacio (por defecto: `data/tarifas/tarifas_<dominio>.csv`)
- Notas adicionales del usuario después de la URL (ej. "utiliza headed", "tablas paginadas")

Ejemplos:
- `/scrape-tarifas https://orler.com.mx/`
- `/scrape-tarifas https://orler.com.mx/ data/tarifas/orler_2026.csv`

---

## Paso 1 – Parsear argumentos y verificar parser existente

Extrae URL y ruta de salida. Construye `<dominio>` desde la URL (ej. `autopistagolfocentro` de `autopistagolfocentro.com`).

**Primero verifica si ya existe un parser dedicado:**

```bash
ls parsers/parser_<dominio>*.py 2>/dev/null
```

Si existe → **ejecútalo directamente** y salta al Paso 7:
```bash
uv run python parsers/parser_<dominio>.py
```

Si no existe, continúa con el proceso normal.

---

## Paso 2 – Abrir el sitio

```bash
playwright-cli open <URL>
playwright-cli snapshot
```

**Detectar antibot:** Si el título es "blocked", "Access Denied", "403" o el snapshot tiene muy poco contenido (solo nav/footer, sin tablas ni texto de tarifas), cierra y reabre con `--headed`:

```bash
playwright-cli close
playwright-cli open --headed <URL>
playwright-cli snapshot
```

Sitios conocidos que requieren `--headed`: tepicmazatlan.com.mx, autopistaguadalajaratepic.com.mx, libramientoslp.mx

---

## Paso 3 – Identificar estructura de la página

Examina el snapshot y clasifica el tipo de contenido:

| Tipo | Señales en snapshot | Patrón de extracción |
|------|---------------------|----------------------|
| **HTML table** | `table [ref=...]` con `row`, `cell` | → Paso 4A |
| **ARIA table** | `table [ref=...]` con `role=table` pero `querySelectorAll('table')=0` | → Paso 4B |
| **Cards h3/h4** | `heading [level=3]` con "Tramo" + `heading [level=4]` con categorías | → Paso 4C |
| **Tabs AJAX** | Lista de tabs con una sola tabla que cambia | → Paso 4D |
| **Imagen** | Solo `img` en `main`, sin tablas ni texto de precios | → Paso 4E |
| **PDF** | Links a `.pdf` con las tarifas | → Paso 4F |
| **Párrafos** | Headings de caseta + párrafos con precios `$XX` | → Paso 4G |

Si hay **acordeones o botones "Ver Tarifa"**, haz clic en todos antes de continuar:
```bash
playwright-cli click <ref_boton_1>
playwright-cli click <ref_boton_2>
playwright-cli snapshot
```

---

## Paso 4A – Extracción: HTML tables

```bash
playwright-cli eval "JSON.stringify(Array.from(document.querySelectorAll('table')).map(function(table) { var title = ''; var firstRow = table.querySelector('tr'); if (firstRow) { var cells = firstRow.querySelectorAll('th,td'); if (cells.length === 1) title = cells[0].textContent.trim(); } if (!title) { var container = table.parentElement; for (var el = container; el && !title; el = el.parentElement) { var hs = Array.from(el.querySelectorAll('h1,h2,h3,h4,h5,h6')).filter(function(h){ return !table.contains(h); }); if (hs.length) { title = hs[0].textContent.trim(); break; } else { var ss = Array.from(el.querySelectorAll('strong')).filter(function(s){ return !table.contains(s); }); if (ss.length) { title = ss[0].textContent.trim(); break; } } } } var rows = Array.from(table.querySelectorAll('tr')).map(function(tr) { return Array.from(tr.querySelectorAll('th,td')).map(function(td) { return td.innerText.replace(/\s+/g,' ').trim(); }); }).filter(function(r) { return r.length >= 2; }); return {title: title, rows: rows}; }))" > /tmp/tarifas_raw.json
```

Luego corre el parser estándar:
```bash
uv run python scripts/parse_tarifas.py /tmp/tarifas_raw.json <URL> <output.csv>
```

**Problemas conocidos y soluciones:**

- **Encabezado doble** (ej. fila "CAMIONES PESADOS" + fila real de vehículos): el parser estándar falla. Usa `rows[1]` como header en lugar de `rows[0]`. Escribe un script Python custom.
- **Tablas responsive duplicadas** (ej. ganamexico): El sitio genera múltiples tablas donde tables[1:] son versiones móviles. Usa solo `tables[0]` (la tabla con más columnas).
- **DataTables con paginación** (ej. circuitoexterior): DataTables mantiene TODAS las filas en el DOM aunque no sean visibles. `querySelectorAll('table tr')` devuelve todas sin necesidad de paginar.
- **Orientación de matriz**: Si el parser produce solo 1 vehicle type por fila, la orientación está invertida. Verifica que `header[0]` contenga "tramo", "autopista", "caseta" o "plaza" (ya manejado en `parse_tarifas.py`).

---

## Paso 4B – Extracción: ARIA tables (role=table)

Cuando `document.querySelectorAll('table').length === 0` pero el snapshot muestra una tabla:

```bash
playwright-cli eval "JSON.stringify(Array.from(document.querySelectorAll('[role=table]')).map(function(table) { var rows = Array.from(table.querySelectorAll('[role=row]')).map(function(tr) { return Array.from(tr.querySelectorAll('[role=columnheader],[role=rowheader],[role=cell]')).map(function(td) { return td.innerText.replace(/\s+/g,' ').trim(); }); }).filter(function(r) { return r.length >= 2; }); return {rows: rows}; }))" > /tmp/tarifas_raw.json
```

Luego parsea con Python custom (el parser estándar no lee ARIA tables).

**Ejemplo** (caseta.mx — Autopista Cardel-Veracruz):
```python
tables = json.loads(json.loads('"' + lines[1].strip()[1:-1] + '"'))
# tables[0]['rows'][0] = header, rows[1:] = data
```

---

## Paso 4C – Extracción: Cards con h3/h4 (sin tablas)

Cuando el snapshot muestra `heading [level=3]` con nombres de tramo y `heading [level=4]` con tipos de vehículo:

```bash
playwright-cli eval "JSON.stringify(Array.from(document.querySelectorAll('h3')).filter(function(h){ return h.textContent.includes('Tramo'); }).map(function(h3) { var container = h3.parentElement; var items = Array.from(container.querySelectorAll('h4')).map(function(h4) { var parts = h4.parentElement.innerText.split('\n').map(function(s){ return s.trim(); }).filter(Boolean); return parts; }); return {tramo: h3.innerText.trim(), items: items}; }))" > /tmp/cards_raw.json
```

El array `items` contiene pares `[categoria, "$XX.XX"]` por tramo.

**Sitios conocidos con esta estructura**: autopistatuxpantampico.com, autopistacardelpozarica.com (mismo operador).

---

## Paso 4D – Extracción: Tabs con carga AJAX

Cuando hay una lista de tabs y solo 1 tabla en el DOM que cambia al hacer clic:

1. Identifica los refs de cada tab en el snapshot
2. Para cada tab:
   ```bash
   playwright-cli click <ref_tab>
   playwright-cli eval "JSON.stringify(Array.from(document.querySelectorAll('table tr')).map(function(tr){ return Array.from(tr.querySelectorAll('th,td')).map(function(td){ return td.innerText.trim(); }); }).filter(function(r){ return r.length>=2; }))"
   ```
3. Acumula los datos de cada tab con su nombre como caseta

**Sitio conocido**: libramientoslp.mx (7 tabs: Oriente, Norte, Poniente, Horizontes-Zacatecas, Horizontes-Guadalajara, Rioverde-La Pila, Ventura-El Peyote)

---

## Paso 4E – Extracción: Imagen con tabla de tarifas

Cuando `main` solo contiene `img` sin HTML tables:

1. Obtén la URL de la imagen:
   ```bash
   playwright-cli eval "document.querySelector('main img') ? document.querySelector('main img').src : ''"
   ```
2. Descarga en alta resolución:
   ```bash
   python3 -c "import urllib.request; urllib.request.urlretrieve('<img_url>', '/tmp/tarifas_img.jpg')"
   ```
3. Lee el archivo con el Read tool para ver la imagen visualmente
4. Transcribe los datos y escribe un parser en `parsers/parser_<dominio>.py`

**Sitios conocidos**: autopistagolfocentro.com, autopistajalacompostela.com

---

## Paso 4F – Extracción: PDF

Cuando hay links a PDF con las tarifas:

```bash
python3 -c "import urllib.request; urllib.request.urlretrieve('<pdf_url>', '/tmp/tarifas.pdf')"
uv run markitdown /tmp/tarifas.pdf -o /tmp/tarifas.md
```

Si `markitdown` no preserva la estructura tabular correctamente (tablas con muchas columnas), usa `pdfplumber` con extracción por posición x de palabras. Guarda el extractor en `parsers/parser_<dominio>.py`.

**Sitios conocidos**: pot.capufe.mx (PDF 10 páginas, pdfplumber), redviacorta.mx / FARAC (PDF 1 página, pdfplumber con word-position bucketing)

---

## Paso 4G – Extracción: Precios en párrafos de texto

Cuando los precios están en `<p>` como texto plano junto a headings de caseta:

Lee los valores directamente del snapshot y escribe el CSV desde Python con datos hardcodeados. Los precios cambian anualmente, documenta la fecha de vigencia.

**Sitio conocido**: libramientocelaya.com.mx (Crespo, Laja, San Miguel × 8 categorías)

---

## Paso 5 – Convertir a CSV (formato estándar)

El CSV de salida siempre tiene estas columnas:
```
url_fuente, autopista, clasificacion, tarifa_mxn, fecha_extraccion
```

- `autopista`: Nombre del operador/concesión (no el nombre de la caseta)
- `clasificacion`: `"{caseta/tramo} | {tipo_vehiculo}"` o `"{tipo_vehiculo}"` si el CSV ya está por caseta
- `tarifa_mxn`: número sin signo `$` ni comas
- `fecha_extraccion`: ISO date `YYYY-MM-DD`

Para tablas estándar usa `scripts/parse_tarifas.py`. Para estructuras custom escribe Python inline o en `parsers/`.

---

## Paso 6 – Guardar parser si fue custom

Si usaste lógica especial (imagen, PDF, ARIA, cards, tabs, texto), guarda el extractor:

```bash
# parsers/parser_<dominio>.py
```

Naming: `parser_autopistagolfocentro.py`, `parser_capufe.py`, etc.

---

## Paso 7 – Cerrar browser y confirmar

```bash
playwright-cli close
```

Reporta: `N filas guardadas en data/tarifas/tarifas_<dominio>.csv`

---

## Referencia rápida: sitios ya resueltos

| Dominio | Tipo | Notas |
|---------|------|-------|
| orler.com.mx | HTML table + acordeones | Parser estándar |
| autopistasdecuota.com | HTML table | Parser estándar, múltiples rutas por URL |
| tepicmazatlan.com.mx | HTML table | **Requiere `--headed`** |
| autopistaguadalajaratepic.com.mx | HTML table | **Requiere `--headed`** |
| pot.capufe.mx | PDF 10 páginas | `parsers/parser_capufe.py` (pdfplumber) |
| autopistajalacompostela.com | Imagen | Parser manual, datos en `parsers/` |
| redviacorta.mx (FARAC) | PDF 1 página | pdfplumber word-position |
| circuitoexterior.mx | HTML table DataTables | Paginación virtual, todos los rows en DOM |
| ganamexico.com.mx | HTML table + tablas responsive | Usar solo `tables[0]` |
| autopistagolfocentro.com | Imagen | `parsers/parser_autopistagolfocentro.py` |
| caseta.mx | ARIA role=table | Extracción con `[role=row]` |
| casmexico.com | HTML table + header doble | Skip fila "CAMIONES PESADOS", usar `rows[1]` como header |
| autopistatuxpantampico.com | Cards h3/h4 | Extracción con `h4.parentElement.innerText` |
| autopistacardelpozarica.com | Cards h3/h4 | Mismo operador que tuxpantampico |
| libramientocelaya.com.mx | Párrafos texto | Datos hardcodeados, vigencia 2026 |
| libramientoirapuato.mx | HTML table | Parser estándar (fix tramo_kw aplicado) |
| libramientoslp.mx | Tabs AJAX | **Requiere `--headed`**, click por tab |
