package main

import (
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// ── Vistas del selector principal (`ksw`) ──────────────
//
// La vista por defecto es la lista plana original. Ctrl+V abre un selector con todas las vistas;
// la elegida se guarda en ~/.ksw.json (view_mode). Todo se deriva localmente del ARN del contexto
// y de ~/.aws/config: no hay llamadas a AWS.

const (
	viewList    = "list"
	viewColumns = "columns"
	viewGrouped = "grouped"
	viewTree    = "tree"
	viewRegion  = "region"
	viewKubectx = "kubectx"
	viewSimple  = "simple"
	viewMatrix  = "matrix"
	viewCards   = "cards"
	viewGrid    = "grid"
	viewSplit   = "split"
)

type viewSpec struct{ name, desc string }

// viewSpecs es el orden en que aparecen en el selector (Ctrl+V).
var viewSpecs = []viewSpec{
	{viewList, "original flat list"},
	{viewKubectx, "fzf-style like kubectx: full names, best match at the bottom"},
	{viewSimple, "list with env and profile"},
	{viewColumns, "table: cluster · env · profile, with env tabs"},
	{viewGrouped, "table grouped by application (dev · qa · pdn together)"},
	{viewTree, "foldable tree by application  (space folds)"},
	{viewRegion, "foldable tree by region  (space folds)"},
	{viewMatrix, "apps × environments  (←↑↓→ move)"},
	{viewCards, "one card per app with all its environments  (←→ card)"},
	{viewGrid, "multi-column names  (←→ change column)"},
	{viewSplit, "list + detail panel"},
}

// minTableWidth es el ancho mínimo de terminal para las vistas nuevas; por debajo se usa la lista original.
const minTableWidth = 90

// minSplitWidth es el ancho mínimo para el panel de detalle; por debajo "split" se dibuja como "columns".
const minSplitWidth = 110

// knownEnvs define el orden de las pestañas de ambiente, de los grupos y de la matriz.
var knownEnvs = []string{"dev", "qa", "pdn", "prod", "sbx", "lab"}

// ctxInfo es la información derivada (sin red) de un contexto del kubeconfig.
type ctxInfo struct {
	Cluster string // nombre del cluster (último segmento del ARN)
	Region  string
	Account string
	Profile string // profile de ~/.aws/config cuyo sso_account_id coincide
	Env     string // dev | qa | pdn | prod | sbx | lab | "" (desconocido)
	EKS     bool
}

// envFromName detecta el ambiente por el sufijo del nombre (p.ej. "eks-payments-pdn" → "pdn").
func envFromName(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx < 0 {
		return ""
	}
	suffix := strings.ToLower(name[idx+1:])
	for _, e := range knownEnvs {
		if suffix == e {
			return e
		}
	}
	return ""
}

// nameOf es el nombre que se muestra del contexto idx: corto o completo según Ctrl+H (en todas las vistas).
func (m *model) nameOf(idx int) string {
	if m.shortNames {
		return m.infos[idx].Cluster
	}
	return m.contexts[idx]
}

// fullBase es el contexto completo sin el sufijo de ambiente (etiqueta de aplicación con nombres completos).
func (m *model) fullBase(idx int) string {
	ctx := m.contexts[idx]
	if m.infos[idx].Env != "" {
		if i := strings.LastIndex(ctx, "-"); i > 0 {
			return ctx[:i]
		}
	}
	return ctx
}

// baseName quita el sufijo de ambiente: "eks-payments-pdn" → "eks-payments".
func baseName(in ctxInfo) string {
	if in.Env != "" {
		if idx := strings.LastIndex(in.Cluster, "-"); idx > 0 {
			return in.Cluster[:idx]
		}
	}
	return in.Cluster
}

// parseContextInfo deriva región, cuenta, profile y ambiente de un contexto.
// Los contextos que no son ARN de EKS (minikube, kind...) conservan su nombre y no tienen cuenta.
func parseContextInfo(ctx string, profileByAccount map[string]string) ctxInfo {
	parts := strings.SplitN(ctx, ":", 6)
	if len(parts) == 6 && parts[0] == "arn" && parts[2] == "eks" && strings.HasPrefix(parts[5], "cluster/") {
		name := strings.TrimPrefix(parts[5], "cluster/")
		return ctxInfo{
			Cluster: name,
			Region:  parts[3],
			Account: parts[4],
			Profile: profileByAccount[parts[4]],
			Env:     envFromName(name),
			EKS:     true,
		}
	}
	name := shortName(ctx)
	return ctxInfo{Cluster: name, Env: envFromName(name)}
}

// selectedProfile retorna el profile de ~/.aws/config del contexto bajo el cursor ("" si no hay).
func (m *model) selectedProfile() string {
	if len(m.infos) != len(m.contexts) || m.cursor < 0 || m.cursor >= len(m.filtered) {
		return ""
	}
	return m.infos[m.filtered[m.cursor]].Profile
}

// profileByAccountID mapea account ID → profile (el primero alfabéticamente si hay varios).
func profileByAccountID() map[string]string {
	details := readProfileDetails()
	names := make([]string, 0, len(details))
	for n := range details {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make(map[string]string, len(names))
	for _, n := range names {
		d := details[n]
		if !d.Valid {
			continue
		}
		if _, ok := out[d.AccountID]; !ok {
			out[d.AccountID] = n
		}
	}
	return out
}

// ── Pestañas ───────────────────────────────────────────

type ctxTab struct {
	label string
	kind  string // all | env | pins | recent
	env   string
}

// buildTabs arma las pestañas: All, los ambientes que existen en el kubeconfig, ★ pins y ⏱ recent.
// Los contextos sin ambiente reconocido se ven en All (no hay pestaña "other").
func buildTabs(infos []ctxInfo) []ctxTab {
	present := make(map[string]bool)
	for _, in := range infos {
		if in.Env != "" {
			present[in.Env] = true
		}
	}
	tabs := []ctxTab{{label: "All", kind: "all"}}
	for _, e := range knownEnvs {
		if present[e] {
			tabs = append(tabs, ctxTab{label: e, kind: "env", env: e})
		}
	}
	tabs = append(tabs, ctxTab{label: "★ pins", kind: "pins"}, ctxTab{label: "⏱ recent", kind: "recent"})
	return tabs
}

// normalizeView valida el modo guardado en la config; el default es la lista original.
func normalizeView(v string) string {
	for _, s := range viewSpecs {
		if s.name == v {
			return v
		}
	}
	return viewList
}

// ev es la vista efectiva: list si la terminal es angosta, y split degrada a columns sin espacio.
func (m *model) ev() string {
	if m.view == viewKubectx {
		return viewKubectx // no necesita ancho mínimo: es solo la lista de nombres
	}
	if m.view == viewList || m.terminalWidth < minTableWidth || len(m.infos) != len(m.contexts) {
		return viewList
	}
	if m.view == viewSplit && m.terminalWidth < minSplitWidth {
		return viewColumns
	}
	return m.view
}

func (m *model) tableMode() bool { return m.ev() != viewList }

// usesTabs: vistas que muestran la barra de pestañas (←→ / Tab las cambian).
func (m *model) usesTabs() bool {
	switch m.ev() {
	case viewColumns, viewGrouped, viewTree, viewRegion, viewSplit, viewGrid:
		return true
	}
	return false
}

// headersActive: vistas con encabezados de grupo. También al buscar: la vista no cambia, solo se filtra.
func (m *model) headersActive() bool {
	switch m.ev() {
	case viewGrouped, viewTree, viewRegion:
		return true
	}
	return false
}

// collapsible: vistas donde Space pliega/despliega grupos.
func (m *model) collapsible() bool {
	switch m.ev() {
	case viewTree, viewRegion:
		return true
	}
	return false
}

// refreshRecent recalcula el ranking de recientes: 0 = contexto actual, luego el historial.
func (m *model) refreshRecent() {
	m.recent = make(map[string]int)
	if m.current != "" {
		m.recent[m.current] = 0
	}
	for i, h := range m.cfg.History {
		if _, ok := m.recent[h]; !ok {
			m.recent[h] = i + 1
		}
	}
}

// inTabSpec indica si el contexto i pertenece a la pestaña t.
func (m *model) inTabSpec(i int, t ctxTab) bool {
	switch t.kind {
	case "env":
		return m.infos[i].Env == t.env
	case "pins":
		return m.isPinned(m.contexts[i])
	case "recent":
		_, ok := m.recent[m.contexts[i]]
		return ok
	}
	return true
}

// inTab aplica la pestaña activa (solo en vistas con pestañas).
func (m *model) inTab(i int) bool {
	if !m.usesTabs() || m.tab >= len(m.tabs) {
		return true
	}
	return m.inTabSpec(i, m.tabs[m.tab])
}

// baseSet indica si el contexto pasa los filtros de grupo / pinned-only (independientes de la pestaña).
func (m *model) baseSet(i int, gs map[string]bool) bool {
	ctx := m.contexts[i]
	if gs != nil && !gs[ctx] {
		return false
	}
	if m.showPinnedOnly && !m.isPinned(ctx) {
		return false
	}
	return true
}

func (m *model) tabCount(t ctxTab) int {
	gs := m.groupSet()
	n := 0
	for i := range m.contexts {
		if m.baseSet(i, gs) && m.inTabSpec(i, t) {
			n++
		}
	}
	return n
}

// ── Orden ──────────────────────────────────────────────

func envRank(env string) int {
	for i, e := range knownEnvs {
		if e == env {
			return i
		}
	}
	return len(knownEnvs)
}

// groupOf devuelve la clave, etiqueta y cuenta del grupo del contexto i según la vista.
// La clave "\xff" (otros contextos) siempre queda al final.
func (m *model) groupOf(i int) (key, label, account string) {
	in := m.infos[i]
	if m.ev() == viewRegion {
		if in.Region != "" {
			return in.Region, in.Region, ""
		}
		return "\xff", "other", ""
	}
	// grouped / tree: por aplicación (los ambientes de una misma app juntos). Agrupar por profile dejaba
	// un grupo por cluster y duplicaba cada fila con un encabezado.
	if in.Env == "" {
		return "\xff", "other", "" // sin ambiente reconocido: un solo grupo al final, no uno por contexto
	}
	app := baseName(in)
	label = strings.TrimPrefix(app, "eks-")
	if label == "" {
		label = app
	}
	if !m.shortNames {
		label = m.fullBase(i)
	}
	return app, label, ""
}

// sortByGroup ordena por grupo, luego ambiente y nombre.
func (m *model) sortByGroup(indices []int) []int {
	out := append([]int(nil), indices...)
	sort.SliceStable(out, func(a, b int) bool {
		ka, _, _ := m.groupOf(out[a])
		kb, _, _ := m.groupOf(out[b])
		if ka != kb {
			return ka < kb
		}
		ia, ib := m.infos[out[a]], m.infos[out[b]]
		if ra, rb := envRank(ia.Env), envRank(ib.Env); ra != rb {
			return ra < rb
		}
		return ia.Cluster < ib.Cluster
	})
	return out
}

// sortMatrix ordena por aplicación y ambiente (el orden de la matriz, fila por fila).
func (m *model) sortMatrix(indices []int) []int {
	out := append([]int(nil), indices...)
	sort.SliceStable(out, func(a, b int) bool {
		ia, ib := m.infos[out[a]], m.infos[out[b]]
		if ba, bb := baseName(ia), baseName(ib); ba != bb {
			return ba < bb
		}
		if ra, rb := envRank(ia.Env), envRank(ib.Env); ra != rb {
			return ra < rb
		}
		return ia.Cluster < ib.Cluster
	})
	return out
}

// promoteRecent deja los pins primero, luego los recientes (más reciente arriba) y luego el resto.
func (m *model) promoteRecent(ordered []int) []int {
	var pins, recents, rest []int
	for _, idx := range ordered {
		ctx := m.contexts[idx]
		switch {
		case m.isPinned(ctx):
			pins = append(pins, idx)
		default:
			if _, ok := m.recent[ctx]; ok {
				recents = append(recents, idx)
			} else {
				rest = append(rest, idx)
			}
		}
	}
	sort.SliceStable(recents, func(a, b int) bool {
		return m.recent[m.contexts[recents[a]]] < m.recent[m.contexts[recents[b]]]
	})
	return append(append(pins, recents...), rest...)
}

// sortRecentTab ordena la pestaña ⏱ recent por recencia.
func (m *model) sortRecentTab(indices []int) []int {
	out := append([]int(nil), indices...)
	sort.SliceStable(out, func(a, b int) bool {
		return m.recent[m.contexts[out[a]]] < m.recent[m.contexts[out[b]]]
	})
	return out
}

// arrange ordena los índices (sin búsqueda activa) según la vista.
func (m *model) arrange(indices []int) []int {
	switch m.ev() {
	case viewList:
		return m.sortedByPins(indices)
	case viewGrouped, viewTree, viewRegion:
		return m.sortByGroup(indices)
	case viewMatrix, viewCards:
		return m.sortMatrix(indices)
	case viewSimple:
		return m.sortedByPins(indices)
	case viewKubectx:
		// Igual que kubectx: orden alfabético; el primero queda abajo, junto al prompt (pins primero)
		sorted := append([]int(nil), indices...)
		sort.SliceStable(sorted, func(a, b int) bool { return m.contexts[sorted[a]] < m.contexts[sorted[b]] })
		return m.sortedByPins(sorted)
	}
	if m.tab < len(m.tabs) && m.tabs[m.tab].kind == "recent" {
		return m.sortRecentTab(indices)
	}
	return m.promoteRecent(m.sortedByPins(indices))
}

// ── Filas con encabezados (grouped / tree / region) ──

type dispRow struct {
	header    bool
	collapsed bool // encabezado plegado: es seleccionable (representa al primer item del grupo)
	label     string
	account   string
	key       string
	count     int
	pos       int  // posición en m.filtered; -1 si es un encabezado desplegado
	last      bool // último item del grupo (glifo └─)
}

// isCollapsed: estado de plegado de un grupo (por defecto todos desplegados).
func (m *model) isCollapsed(key string) bool {
	if m.search != "" {
		return false // al buscar se muestran todos los resultados, sin grupos plegados
	}
	return m.toggled[m.ev()+"|"+key]
}

// layoutRows convierte la lista ordenada en m.filtered (items navegables) y m.rows (filas de pantalla).
// Un grupo plegado deja un solo item navegable (su primer elemento) para poder desplegarlo.
func (m *model) layoutRows(full []int) ([]int, []dispRow) {
	if !m.headersActive() {
		return full, nil
	}
	var filtered []int
	var rows []dispRow
	for i := 0; i < len(full); {
		key, label, acc := m.groupOf(full[i])
		j := i
		for j < len(full) {
			k, _, _ := m.groupOf(full[j])
			if k != key {
				break
			}
			j++
		}
		if m.collapsible() && m.isCollapsed(key) {
			rows = append(rows, dispRow{header: true, collapsed: true, label: label, account: acc, key: key, count: j - i, pos: len(filtered)})
			filtered = append(filtered, full[i])
		} else {
			rows = append(rows, dispRow{header: true, label: label, account: acc, key: key, count: j - i, pos: -1})
			for k := i; k < j; k++ {
				rows = append(rows, dispRow{key: key, pos: len(filtered), last: k == j-1})
				filtered = append(filtered, full[k])
			}
		}
		i = j
	}
	return filtered, rows
}

// orderForSearch ordena los resultados de una búsqueda (ya ordenados por puntaje) para las vistas con grupos:
// los grupos se ordenan por su mejor coincidencia y, dentro de cada grupo, se conserva el orden por puntaje.
func (m *model) orderForSearch(byScore []int) []int {
	var keys []string
	buckets := make(map[string][]int)
	for _, idx := range byScore {
		k, _, _ := m.groupOf(idx)
		if _, ok := buckets[k]; !ok {
			keys = append(keys, k)
		}
		buckets[k] = append(buckets[k], idx)
	}
	out := make([]int, 0, len(byScore))
	for _, k := range keys {
		out = append(out, buckets[k]...)
	}
	return out
}

// rowAtCursor devuelve la fila de pantalla del cursor (item o encabezado plegado).
func (m *model) rowAtCursor() (dispRow, bool) {
	for _, r := range m.rows {
		if r.pos == m.cursor && (!r.header || r.collapsed) {
			return r, true
		}
	}
	return dispRow{}, false
}

// toggleGroup pliega/despliega un grupo y deja el cursor en un lugar razonable.
func (m *model) toggleGroup(key string) {
	k := m.ev() + "|" + key
	m.toggled[k] = !m.toggled[k]
	m.resetFilter()
	for i, r := range m.rows {
		if r.header && r.key == key {
			if r.collapsed {
				m.cursor = r.pos
			} else if i+1 < len(m.rows) {
				m.cursor = m.rows[i+1].pos
			}
			break
		}
	}
	if m.cursor >= len(m.filtered) {
		m.cursor = max(0, len(m.filtered)-1)
	}
	m.ensureVisible()
}

// ── Matriz (aplicaciones × ambientes) ──────────────────

type matrixRow struct {
	app   string
	label string // lo que se muestra: app corta o contexto completo sin sufijo (Ctrl+H)
	cells []int  // posición en m.filtered por columna, -1 si no existe
}

// matrixLayout arma la matriz con lo que hay en m.filtered (ya ordenado por sortMatrix).
// Si una aplicación tiene dos clusters en el mismo ambiente (cuentas distintas) se repite la fila.
func (m *model) matrixLayout() (cols []string, rows []matrixRow) {
	present := make(map[string]bool)
	noEnv := false
	for _, idx := range m.filtered {
		if e := m.infos[idx].Env; e != "" {
			present[e] = true
		} else {
			noEnv = true
		}
	}
	for _, e := range knownEnvs {
		if present[e] {
			cols = append(cols, e)
		}
	}
	if noEnv {
		cols = append(cols, "")
	}
	colOf := make(map[string]int, len(cols))
	for i, c := range cols {
		colOf[c] = i
	}
	appStart := 0
	for pos, idx := range m.filtered {
		in := m.infos[idx]
		app := baseName(in)
		c := colOf[in.Env]
		if len(rows) == 0 || rows[len(rows)-1].app != app {
			appStart = len(rows)
		}
		placed := false
		for r := appStart; r < len(rows); r++ {
			if rows[r].cells[c] < 0 {
				rows[r].cells[c] = pos
				placed = true
				break
			}
		}
		if !placed {
			cells := make([]int, len(cols))
			for i := range cells {
				cells[i] = -1
			}
			cells[c] = pos
			label := app
			if !m.shortNames {
				label = m.fullBase(idx)
			}
			rows = append(rows, matrixRow{app: app, label: label, cells: cells})
		}
	}
	return cols, rows
}

// matrixMove mueve el cursor a la celda vecina con contenido (dr filas, dc columnas).
func (m *model) matrixMove(dr, dc int) {
	_, rows := m.matrixLayout()
	r, c := -1, -1
	for i, row := range rows {
		for j, pos := range row.cells {
			if pos == m.cursor {
				r, c = i, j
			}
		}
	}
	if r < 0 {
		return
	}
	if dc != 0 {
		for j := c + dc; j >= 0 && j < len(rows[r].cells); j += dc {
			if rows[r].cells[j] >= 0 {
				m.cursor = rows[r].cells[j]
				m.mcol, m.mcolSet = j, true
				break
			}
		}
	} else {
		// Al subir/bajar se recuerda la columna elegida aunque una fila intermedia no tenga ese ambiente
		if m.mcolSet {
			c = m.mcol
		}
		for i := r + dr; i >= 0 && i < len(rows); i += dr {
			best, bestDist := -1, 1<<30
			for j, pos := range rows[i].cells {
				if pos < 0 {
					continue
				}
				d := j - c
				if d < 0 {
					d = -d
				}
				if d < bestDist {
					best, bestDist = pos, d
				}
			}
			if best >= 0 {
				m.cursor = best
				break
			}
		}
	}
	m.ensureVisible()
}

// ── Tarjetas (una por aplicación) ──────────────────────

// presentEnvs devuelve los ambientes que existen en el kubeconfig (orden estándar) y si hay contextos sin ambiente.
func (m *model) presentEnvs() (envs []string, noEnv bool) {
	present := make(map[string]bool)
	for _, in := range m.infos {
		if in.Env != "" {
			present[in.Env] = true
		} else {
			noEnv = true
		}
	}
	for _, e := range knownEnvs {
		if present[e] {
			envs = append(envs, e)
		}
	}
	return envs, noEnv
}

// cardJump salta a la primera fila de la tarjeta anterior (dir -1) o siguiente (dir +1).
func (m *model) cardJump(dir int) {
	lines := m.buildCards()
	cur := -1
	for _, l := range lines {
		if !l.header && l.pos == m.cursor {
			cur = l.card
			break
		}
	}
	if cur < 0 {
		return
	}
	target := cur + dir
	for _, l := range lines {
		if !l.header && l.card == target {
			m.cursor = l.pos
			m.ensureVisible()
			return
		}
	}
}

// ── Grid (columnas de nombres) ─────────────────────────

// gridGeometry calcula el ancho de columna, cuántas columnas caben y cuántas filas tiene cada una.
func (m *model) gridGeometry() (colW, visCols, rows int) {
	maxName := 8
	for _, idx := range m.filtered {
		if l := len([]rune(m.nameOf(idx))); l > maxName {
			maxName = l
		}
	}
	capW := 34
	if !m.shortNames {
		capW = 70 // con nombres completos (ARN) las columnas son más anchas
	}
	if maxName > capW {
		maxName = capW
	}
	colW = maxName + 5
	visCols = max(1, (m.terminalWidth-4)/colW)
	rows = max(1, m.maxVisible()+2) // sin cabecera de columnas ni indicadores ▲▼ propios
	return
}

func (m *model) gridMove(dir int) {
	_, _, rows := m.gridGeometry()
	n := len(m.filtered)
	if n == 0 {
		return
	}
	target := m.cursor + dir*rows
	switch {
	case target < 0:
		return
	case target >= n:
		if m.cursor/rows >= (n-1)/rows { // ya está en la última columna
			return
		}
		target = n - 1
	}
	m.cursor = target
	m.ensureVisible()
}

// ── Scroll específico por vista ────────────────────────

// ensureVisibleView mantiene el cursor visible en las vistas con geometría propia.
// Retorna false si la vista usa el scroll estándar de la lista.
func (m *model) ensureVisibleView() bool {
	mv := m.maxVisible()
	switch {
	case m.headersActive():
		cur := 0
		for i, r := range m.rows {
			if r.pos == m.cursor && (!r.header || r.collapsed) {
				cur = i
				break
			}
		}
		top := cur
		if cur > 0 && m.rows[cur-1].header && !m.rows[cur-1].collapsed {
			top = cur - 1 // mostrar el encabezado junto al primer item del grupo
		}
		if top < m.scrollOffset {
			m.scrollOffset = top
		} else if cur >= m.scrollOffset+mv {
			m.scrollOffset = cur - mv + 1
		}
	case m.ev() == viewMatrix:
		_, rows := m.matrixLayout()
		cur := 0
		for i, row := range rows {
			for _, pos := range row.cells {
				if pos == m.cursor {
					cur = i
				}
			}
		}
		if cur < m.scrollOffset {
			m.scrollOffset = cur
		} else if cur >= m.scrollOffset+mv {
			m.scrollOffset = cur - mv + 1
		}
	case m.ev() == viewCards:
		lines := m.buildCards()
		cur := 0
		for i, l := range lines {
			if !l.header && l.pos == m.cursor {
				cur = i
				break
			}
		}
		top := cur
		if cur > 0 && lines[cur-1].header {
			top = cur - 1 // mostrar el encabezado de la tarjeta junto a su primera fila
		}
		bottom := cur
		if cur+1 < len(lines) && lines[cur+1].pos < 0 && !lines[cur+1].header {
			bottom = cur + 1 // la fila siguiente es el borde inferior de la tarjeta: mostrarlo también
		}
		if top < m.scrollOffset {
			m.scrollOffset = top
		} else if bottom >= m.scrollOffset+mv {
			m.scrollOffset = bottom - mv + 1
		}
	case m.ev() == viewGrid:
		_, visCols, rows := m.gridGeometry()
		col := m.cursor / rows
		if col < m.scrollOffset {
			m.scrollOffset = col
		} else if col >= m.scrollOffset+visCols {
			m.scrollOffset = col - visCols + 1
		}
	default:
		return false
	}
	if m.scrollOffset < 0 {
		m.scrollOffset = 0
	}
	return true
}

// ── Cambios de vista / pestañas ────────────────────────

// refilterKeepCursor recalcula la lista conservando el contexto seleccionado.
func (m *model) refilterKeepCursor() {
	selected := ""
	if m.cursor < len(m.filtered) {
		selected = m.contexts[m.filtered[m.cursor]]
	}
	if m.search == "" {
		m.resetFilter()
	} else {
		m.applyFilter()
	}
	m.cursor = 0
	for i, idx := range m.filtered {
		if m.contexts[idx] == selected {
			m.cursor = i
			break
		}
	}
	m.scrollOffset = 0
	m.mcolSet = false
	m.ensureVisible()
}

func (m *model) switchTab(delta int) {
	if !m.usesTabs() || len(m.tabs) == 0 {
		return
	}
	m.tab = (m.tab + delta + len(m.tabs)) % len(m.tabs)
	m.cursor = 0
	m.scrollOffset = 0
	if m.search == "" {
		m.resetFilter()
	} else {
		m.applyFilter()
	}
}

// applyView activa una vista, la guarda en la config y conserva el contexto seleccionado.
func (m *model) applyView(name string) {
	m.view = normalizeView(name)
	m.cfg.ViewMode = m.view
	_ = saveConfig(m.cfg)
	m.tab = 0
	m.refilterKeepCursor()
}

func (m *model) openPicker() {
	m.picker = true
	m.pickerCur = 0
	for i, s := range viewSpecs {
		if s.name == m.view {
			m.pickerCur = i
		}
	}
}

func (m *model) pickerKey(msg tea.KeyMsg) (bool, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return false, nil // ctrl+c siempre sale
	case tea.KeyUp:
		if m.pickerCur > 0 {
			m.pickerCur--
		}
	case tea.KeyDown, tea.KeyCtrlV, tea.KeyTab:
		m.pickerCur = (m.pickerCur + 1) % len(viewSpecs)
	case tea.KeyEnter:
		m.picker = false
		m.applyView(viewSpecs[m.pickerCur].name)
	case tea.KeyEsc:
		m.picker = false
	}
	return true, nil
}

// kubectxKey invierte la navegación: en esta vista la lista crece hacia arriba (índice 0 abajo, junto al prompt),
// así que "arriba" en pantalla es un índice mayor.
func (m *model) kubectxKey(msg tea.KeyMsg) bool {
	n := len(m.filtered)
	if n == 0 {
		return false
	}
	switch msg.Type {
	case tea.KeyUp:
		m.cursor = min(n-1, m.cursor+1)
	case tea.KeyDown:
		m.cursor = max(0, m.cursor-1)
	case tea.KeyPgUp:
		m.cursor = min(n-1, m.cursor+10)
	case tea.KeyPgDown:
		m.cursor = max(0, m.cursor-10)
	case tea.KeyHome, tea.KeyCtrlA: // extremo superior de la pantalla
		m.cursor = n - 1
	case tea.KeyEnd, tea.KeyCtrlE: // extremo inferior, junto al prompt
		m.cursor = 0
	default:
		return false
	}
	m.ensureVisible()
	return true
}

// handleViewKey procesa las teclas propias de las vistas. Retorna true si la tecla fue consumida.
func (m *model) handleViewKey(msg tea.KeyMsg) (bool, tea.Cmd) {
	if m.picker {
		return m.pickerKey(msg)
	}
	if msg.Type == tea.KeyCtrlV {
		m.openPicker()
		return true, nil
	}
	ev := m.ev()
	if ev == viewKubectx && m.kubectxKey(msg) {
		return true, nil
	}
	switch msg.Type {
	case tea.KeyLeft, tea.KeyRight:
		dir := 1
		if msg.Type == tea.KeyLeft {
			dir = -1
		}
		switch {
		case ev == viewMatrix:
			m.matrixMove(0, dir)
			return true, nil
		case ev == viewGrid:
			m.gridMove(dir)
			return true, nil
		case ev == viewCards:
			m.cardJump(dir)
			return true, nil
		case m.usesTabs():
			m.switchTab(dir)
			return true, nil
		}
	case tea.KeyTab, tea.KeyShiftTab:
		if m.usesTabs() {
			dir := 1
			if msg.Type == tea.KeyShiftTab {
				dir = -1
			}
			m.switchTab(dir)
			return true, nil
		}
	case tea.KeyUp, tea.KeyDown:
		if ev == viewMatrix {
			dir := 1
			if msg.Type == tea.KeyUp {
				dir = -1
			}
			m.matrixMove(dir, 0)
			return true, nil
		}
	case tea.KeySpace:
		if m.collapsible() && m.headersActive() && m.search == "" {
			if r, ok := m.rowAtCursor(); ok {
				m.toggleGroup(r.key)
			}
			return true, nil
		}
	case tea.KeyEnter:
		// Enter sobre un grupo plegado lo despliega en vez de cambiar de contexto
		if m.headersActive() && m.collapsible() {
			if r, ok := m.rowAtCursor(); ok && r.header && r.collapsed {
				m.toggleGroup(r.key)
				return true, nil
			}
		}
	}
	return false, nil
}
