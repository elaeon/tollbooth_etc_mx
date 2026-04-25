---
description: Extrae tarifas de autopistas de un sitio web operador y guarda como CSV
argument-hint: <URL> [archivo_salida.csv]
allowed-tools: Bash(playwright-cli:*), Bash(uv run python:*), Bash(mkdir:*), Read, Write, Bash(uv run markitdown)
---

# Scrape Tarifas

Extrae tarifas de peaje de un sitio web operador de autopistas mexicanas y las guarda en CSV.

## Argumentos

`$ARGUMENTS` puede contener:
- URL del sitio operador (por defecto: `https://orler.com.mx/`)
- Opcionalmente, ruta del CSV de salida separada por espacio (por defecto: `data/tarifas/tarifas_<dominio>.csv`)

Ejemplos:
- `/scrape-tarifas` → usa orler.com.mx, guarda en data/tarifas/tarifas_orler.csv
- `/scrape-tarifas https://orler.com.mx/` → igual
- `/scrape-tarifas https://orler.com.mx/ data/tarifas/orler_2026.csv`

## Proceso

### Paso 1 – Parsear argumentos

Extrae URL y ruta de salida de `$ARGUMENTS`. Si no hay URL, usa `https://orler.com.mx/`. Si no hay ruta de salida, construye una con el dominio de la URL. Crea el directorio de salida si no existe.

### Paso 2 – Abrir el sitio y obtener snapshot inicial

```bash
playwright-cli open <URL>
playwright-cli snapshot
```

Examina el snapshot para identificar:
- Tabs de navegación (buscar tabs con texto como "TARIFA", "TARIFAS", "PRECIOS")
- Botones de acordeón para expandir secciones de tarifas
- Tablas ya visibles

### Paso 3 – Navegar a la sección de tarifas

Si hay un tab de tarifas, haz clic en él:
```bash
playwright-cli click <ref_del_tab_tarifa>
playwright-cli snapshot
```

### Paso 4 – Expandir todas las tablas de tarifas

Busca botones de "Ver Tarifa", "Ver más", "Expandir" u otros acordeones que revelen tablas.
Haz clic en **cada uno** de ellos:
```bash
playwright-cli click <ref_boton_1>
playwright-cli click <ref_boton_2>
# ... etc para cada botón encontrado
```

Después toma un snapshot final para confirmar que todas las tablas están visibles.

### Paso 5 – Extraer los datos con JavaScript y guardar raw

Extrae todas las tablas con sus títulos y guarda el output en `/tmp/tarifas_raw.json`:

```bash
playwright-cli eval "JSON.stringify(Array.from(document.querySelectorAll('table')).map(function(table) { var title = ''; var firstRow = table.querySelector('tr'); if (firstRow) { var cells = firstRow.querySelectorAll('th,td'); if (cells.length === 1) title = cells[0].textContent.trim(); } if (!title) { var container = table.parentElement; for (var el = container; el && !title; el = el.parentElement) { var hs = Array.from(el.querySelectorAll('h1,h2,h3,h4,h5,h6')).filter(function(h){ return !table.contains(h); }); if (hs.length) { title = hs[0].textContent.trim(); } else { var ss = Array.from(el.querySelectorAll('strong')).filter(function(s){ return !table.contains(s); }); if (ss.length) title = ss[0].textContent.trim(); } } } var rows = Array.from(table.querySelectorAll('tr')).map(function(tr) { return Array.from(tr.querySelectorAll('th,td')).map(function(td) { return td.innerText.replace(/\s+/g,' ').trim(); }); }).filter(function(r) { return r.length >= 2; }); return {title: title, rows: rows}; }))" > /tmp/tarifas_raw.json
```

### Paso 6 – Convertir a CSV con el script helper

Usa el script `scripts/parse_tarifas.py` para parsear el output y generar el CSV:

```bash
uv run python scripts/parse_tarifas.py /tmp/tarifas_raw.json <URL> <ruta_salida.csv>
```

El script:
- Extrae el nombre de la autopista de los títulos de acordeón (busca "para la/el ...")
- Omite filas de encabezado (CLASIFICACIÓN, TIPO, etc.)
- Limpia los valores de tarifa (quita `$`, comas)
- Genera columnas: `url_fuente`, `autopista`, `clasificacion`, `tarifa_mxn`, `fecha_extraccion`

### Paso 7 – Cerrar el browser y confirmar

```bash
playwright-cli close
```

Muestra un resumen de filas guardadas y la ruta del CSV.

## Notas

- Si el sitio usa una estructura diferente (ej. tablas en iframes, cargas AJAX lentas), toma más snapshots y ajusta los clics según los refs visibles.
- Si `playwright-cli eval` devuelve un string muy largo, guárdalo en un archivo temporal con `playwright-cli eval "..." > /tmp/tarifas_raw.json` y luego léelo con Python.
- Algunos sitios cargan tarifas en PDFs vinculados; en ese caso usa `playwright-cli pdf` para descargar y notifica al usuario que se requiere extracción manual de PDF.
- Si se necesita leer PDFs puedes usar uv run markitdown *.pdf -o *.md para transformarlos a markdown.
