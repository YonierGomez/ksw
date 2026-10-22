package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// ── Estilos ────────────────────────────────────────────

var (
	envStyles = map[string]lipgloss.Style{
		"dev":  lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b")),
		"qa":   lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c")),
		"pdn":  lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#ff5555")),
		"prod": lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#ff5555")),
		"sbx":  lipgloss.NewStyle().Foreground(lipgloss.Color("#8be9fd")),
		"lab":  lipgloss.NewStyle().Foreground(lipgloss.Color("#bd93f9")),
	}
	recentTag    = lipgloss.NewStyle().Foreground(lipgloss.Color("#8be9fd")).Render("⏱")
	tabActiveSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
	tabNormalSt  = lipgloss.NewStyle().Foreground(lipgloss.Color("#999"))
	groupHeadSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#bd93f9"))
	columnHeadSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	groupLabelSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#bd93f9")) // encabezados de grupo: sin negrita
)

// padStyled recorta text a w columnas, le aplica el estilo y rellena con espacios.
func padStyled(text string, w int, st lipgloss.Style) string {
	text = truncateCell(text, w)
	pad := w - lipgloss.Width(text)
	if pad < 0 {
		pad = 0
	}
	return st.Render(text) + strings.Repeat(" ", pad)
}

// padRight rellena con espacios hasta w columnas visibles (el texto puede traer colores).
func padRight(s string, w int) string {
	if pad := w - lipgloss.Width(s); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}

// truncateLeft recorta por la izquierda ("…:cluster/eks-x-dev"): con nombres completos lo importante es el final.
func truncateLeft(s string, w int) string {
	if w <= 0 || lipgloss.Width(s) <= w {
		return s
	}
	r := []rune(s)
	for len(r) > 0 && lipgloss.Width(string(r))+1 > w {
		r = r[1:]
	}
	return "…" + string(r)
}

// padName es padStyled para nombres de contexto: con nombres completos recorta por la izquierda.
func padName(text string, w int, st lipgloss.Style, full bool) string {
	if full {
		text = truncateLeft(text, w)
	} else {
		text = truncateCell(text, w)
	}
	pad := w - lipgloss.Width(text)
	if pad < 0 {
		pad = 0
	}
	return st.Render(text) + strings.Repeat(" ", pad)
}

func envBadge(env string, w int) string {
	if env == "" {
		return padStyled("—", w, dimStyle)
	}
	return padStyled(strings.ToUpper(env), w, envStyles[env])
}

func envStyle(env string) lipgloss.Style {
	if st, ok := envStyles[env]; ok {
		return st
	}
	return dimStyle
}

// wrapText parte s en líneas de a lo sumo w columnas.
func wrapText(s string, w int) []string {
	if w < 8 {
		w = 8
	}
	var out []string
	r := []rune(s)
	for len(r) > w {
		out = append(out, string(r[:w]))
		r = r[w:]
	}
	return append(out, string(r))
}

// ── Entrada principal ──────────────────────────────────

// View elige la vista activa; la lista original se conserva intacta (viewList en main.go).
func (m model) View() string {
	if m.quitting || m.chosen != "" {
		return ""
	}
	if m.picker {
		return m.viewPicker()
	}
	switch m.ev() {
	case viewList:
		return m.viewList()
	case viewMatrix:
		return m.viewMatrix()
	case viewGrid:
		return m.viewGrid()
	case viewSplit:
		return m.viewSplit()
	case viewCards:
		return m.viewCards()
	case viewSimple:
		return m.viewSimple()
	case viewKubectx:
		return m.viewKubectx()
	}
	return m.viewTable()
}

// ── Piezas compartidas ─────────────────────────────────

func (m model) tabsLine() string {
	var parts []string
	for i, t := range m.tabs {
		label := fmt.Sprintf("%s %d", t.label, m.tabCount(t))
		if i == m.tab {
			parts = append(parts, tabActiveSt.Render("[ "+label+" ]"))
		} else {
			parts = append(parts, tabNormalSt.Render(label))
		}
	}
	return strings.Join(parts, "  ")
}

// headLines: contexto actual, detalle del seleccionado (profile · cuenta · región y ARN), búsqueda,
// pestañas (o subtítulo de la vista) y separador. Toda la información del cluster queda arriba.
func (m model) headLines(withDetail bool) []string {
	w := m.terminalWidth
	currentName := m.current
	if m.shortNames {
		currentName = shortName(m.current)
	}
	if alias := m.aliasFor(m.current); alias != "" {
		currentName += " " + aliasStyle.Render("@"+alias)
	}
	filterLabel := ""
	if m.activeGroup != "" {
		filterLabel = "  " + pinItemStyle.Render("["+m.activeGroup+"]")
	} else if m.showPinnedOnly {
		filterLabel = "  " + pinItemStyle.Render("[★ pinned]")
	}
	lines := []string{"  " + currentLabelStyle.Render("  current ") + currentValueStyle.Render(currentName) + filterLabel}
	if withDetail {
		lines = append(lines, m.detailLines()...)
	} else {
		lines = append(lines, "")
	}
	if m.search != "" {
		lines = append(lines, "  "+searchActiveStyle.Render("  ❯ "+m.search+"█"))
	} else {
		lines = append(lines, "  "+searchPlaceholderStyle.Render("  ❯ type to search..."))
	}
	if m.usesTabs() {
		lines = append(lines, "    "+lipgloss.NewStyle().MaxWidth(max(10, w-6)).Render(m.tabsLine()))
	}
	// (las vistas sin pestañas no llevan subtítulo: esa fila se usa para mostrar más clusters)
	lines = append(lines, "  "+dimStyle.Render(strings.Repeat("─", max(10, w-6))))
	return lines
}

// colLayout son los anchos de las columnas de las vistas tipo tabla.
type colLayout struct {
	clusterW, envW, profileW, regionW int
	showProfile, showRegion           bool
}

const tableGap = 2

// colLayoutFor calcula las columnas para un ancho disponible (sin contar sangría ni puntero).
func (m model) colLayoutFor(avail int, wantProfile bool) colLayout {
	multiRegion := false
	maxCluster, maxProfile, maxRegion := 7, 7, 6
	firstRegion := ""
	for i := range m.contexts {
		in := m.infos[i]
		if l := lipgloss.Width(in.Profile); l > maxProfile {
			maxProfile = l
		}
		if in.Region != "" {
			if firstRegion == "" {
				firstRegion = in.Region
			} else if in.Region != firstRegion {
				multiRegion = true
			}
			if l := lipgloss.Width(in.Region); l > maxRegion {
				maxRegion = l
			}
		}
	}
	// El ancho de la columna de nombres sale de lo que se está mostrando (con la lista filtrada se ajusta),
	// para que ENV y profile no queden lejos del nombre.
	if len(m.filtered) > 0 {
		maxCluster = 7
		for _, idx := range m.filtered {
			if l := lipgloss.Width(m.nameOf(idx)); l > maxCluster {
				maxCluster = l
			}
		}
	} else {
		for i := range m.contexts {
			maxCluster = max(maxCluster, lipgloss.Width(m.nameOf(i)))
		}
	}
	c := colLayout{envW: 4, showProfile: wantProfile}
	c.showRegion = multiRegion && wantProfile && !(m.headersActive() && m.ev() == viewRegion)
	if c.showProfile {
		c.profileW = min(maxProfile, 30)
	}
	if c.showRegion {
		c.regionW = maxRegion
	}
	const tagW = 6
	fixed := 5 + c.envW + tagW + tableGap // indent+puntero + env + tags + hueco antes de env
	if c.showProfile {
		fixed += c.profileW + tableGap
	}
	if c.showRegion {
		fixed += c.regionW + tableGap
	}
	// Con nombres cortos la columna se limita a 40: ENV (que ya muestra el sufijo) queda cerca del nombre en lugar
	// de flotar lejos por culpa de un único nombre larguísimo. +3: espacio para el glifo de árbol / sangría.
	if m.shortNames {
		maxCluster = min(maxCluster, 40)
	}
	c.clusterW = min(maxCluster+3, avail-fixed)
	if c.clusterW < 16 {
		c.clusterW = 16
	}
	return c
}

func (c colLayout) headerRow() string {
	h := "  " + "   " + padStyled("CLUSTER", c.clusterW, columnHeadSt) + strings.Repeat(" ", tableGap) + padStyled("ENV", c.envW, columnHeadSt)
	if c.showProfile {
		h += strings.Repeat(" ", tableGap) + padStyled("PROFILE", c.profileW, columnHeadSt)
	}
	if c.showRegion {
		h += strings.Repeat(" ", tableGap) + padStyled("REGION", c.regionW, columnHeadSt)
	}
	return h
}

// tagsFor: ● actual, ★ pin, ⏱ reciente y @alias.
func (m model) tagsFor(ctx string) string {
	tags := ""
	isActive := ctx == m.current
	if isActive {
		tags += " " + activeTag
	}
	if m.isPinned(ctx) {
		tags += " " + pinTag
	}
	if _, ok := m.recent[ctx]; ok && !isActive {
		tags += " " + recentTag
	}
	if alias := m.aliasFor(ctx); alias != "" {
		tags += " " + aliasStyle.Render("@"+alias)
	}
	return tags
}

func (m model) nameStyle(ctx string, selected bool) lipgloss.Style {
	switch {
	case selected:
		return selectedItemStyle
	case ctx == m.current:
		return activeItemStyle
	case m.isPinned(ctx):
		return pinItemStyle
	}
	return normalItemStyle
}

// itemLine dibuja un item en las vistas tipo tabla. prefix va antes del nombre (sangría, glifo de árbol, número).
func (m model) itemLine(c colLayout, pos int, prefix string) string {
	idx := m.filtered[pos]
	ctx := m.contexts[idx]
	in := m.infos[idx]
	selected := pos == m.cursor
	pointer := "   "
	if selected {
		pointer = " ❯ "
	}
	nameW := max(8, c.clusterW-lipgloss.Width(prefix))
	line := "  " + pointer + prefix + padName(m.nameOf(idx), nameW, m.nameStyle(ctx, selected), !m.shortNames) +
		strings.Repeat(" ", tableGap) + envBadge(in.Env, c.envW)
	if c.showProfile {
		line += strings.Repeat(" ", tableGap) + padStyled(in.Profile, c.profileW, dimStyle)
	}
	if c.showRegion {
		line += strings.Repeat(" ", tableGap) + padStyled(in.Region, c.regionW, dimStyle)
	}
	return line + m.tagsFor(ctx)
}

// headerRowLine dibuja un encabezado de grupo (▾ desplegado, ▸ plegado y seleccionable).
func (m model) headerRowLine(r dispRow) string {
	pointer := "   "
	glyph := "▾"
	if r.collapsed {
		glyph = "▸"
		if r.pos == m.cursor {
			pointer = " ❯ "
		}
	}
	line := "  " + pointer + groupLabelSt.Render(glyph+" "+truncateCell(r.label, max(10, m.terminalWidth-20)))
	return line + " " + dimStyle.Render(fmt.Sprintf("%d", r.count))
}

// prefixFor decide la sangría / glifo de cada item según la vista.
func (m model) prefixFor(r dispRow) string {
	switch m.ev() {
	case viewGrouped:
		return "  "
	case viewTree, viewRegion:
		if r.last {
			return dimStyle.Render("└─ ")
		}
		return dimStyle.Render("├─ ")
	}
	return ""
}

// displayRows devuelve las filas de pantalla: con encabezados o una por item.
func (m model) displayRows() []dispRow {
	if m.headersActive() && m.rows != nil {
		return m.rows
	}
	rows := make([]dispRow, len(m.filtered))
	for i := range m.filtered {
		rows[i] = dispRow{pos: i}
	}
	return rows
}

// window calcula el tramo [start,end) de filas a dibujar. Reserva 2 líneas para los indicadores ▲▼, pero solo
// se pintan cuando hay más filas arriba/abajo; las líneas que no se usan se aprovechan con filas extra,
// de modo que la lista llene la pantalla hasta el pie.
func (m model) window(total int) (start, end int) {
	mv := m.maxVisible()
	start = m.scrollOffset
	if start > total {
		start = 0
	}
	end = min(start+mv, total)
	for {
		ind := 0
		if start > 0 {
			ind++
		}
		if end < total {
			ind++
		}
		if (end-start)+ind >= mv+2 {
			break
		}
		if end < total {
			end++
		} else if start > 0 {
			start--
		} else {
			break
		}
	}
	return start, end
}

// bodyRows dibuja cabecera de columnas + filas visibles + indicadores de scroll.
func (m model) bodyRows(c colLayout) []string {
	lines := []string{c.headerRow()}
	rows := m.displayRows()
	start, end := m.window(len(rows))
	if start > 0 {
		lines = append(lines, "  "+dimStyle.Render(fmt.Sprintf("    ▲ %d more", start)))
	}
	for _, r := range rows[start:end] {
		if r.header {
			lines = append(lines, m.headerRowLine(r))
		} else {
			lines = append(lines, m.itemLine(c, r.pos, m.prefixFor(r)))
		}
	}
	if end < len(rows) {
		lines = append(lines, "  "+dimStyle.Render(fmt.Sprintf("    ▼ %d more", len(rows)-end)))
	}
	return lines
}

// detailLines: dos líneas alineadas bajo "current" con la información del seleccionado.
//
//	profile sufi-dev · account 034065175492 · us-east-1
//	arn     arn:aws:eks:us-east-1:034065175492:cluster/eks-sufi-dev
func (m model) detailLines() []string {
	w := m.terminalWidth
	row := func(label, value string, st lipgloss.Style) string {
		return "  " + currentLabelStyle.Render(fmt.Sprintf("  %-7s ", label)) + st.Render(truncateCell(value, max(10, w-16)))
	}
	if len(m.filtered) == 0 {
		return []string{row("profile", "—", dimStyle), row("arn", "—", dimStyle)}
	}
	cur := min(m.cursor, len(m.filtered)-1)
	// Sobre un grupo plegado el detalle describe el grupo, no su primer cluster
	if r, ok := m.rowAtCursor(); ok && r.header && r.collapsed && m.headersActive() {
		return []string{
			row("group", fmt.Sprintf("%s · %s", r.label, plural(r.count, "context", "contexts")), normalItemStyle),
			row("", "enter / space to expand", dimStyle),
		}
	}
	idx := m.filtered[cur]
	sel := m.contexts[idx]
	si := m.infos[idx]
	var parts []string
	if si.EKS {
		p := "no profile"
		if si.Profile != "" {
			p = si.Profile
		}
		parts = append(parts, p, "account "+si.Account, si.Region)
	} else {
		parts = append(parts, "kubeconfig context")
	}
	if alias := m.aliasFor(sel); alias != "" {
		parts = append(parts, "@"+alias)
	}
	label := "profile"
	if !si.EKS {
		label = "type"
	}
	return []string{row(label, strings.Join(parts, " · "), normalItemStyle), row("arn", sel, dimStyle)}
}

// keyHint es una tecla del pie con su descripción; prio 0 = imprescindible, mayor = se descarta antes.
type keyHint struct {
	key, label string
	prio       int
}

var (
	hintKeySt   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#8be9fd"))
	hintLabelSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
)

// footerLine arma el pie en tres bloques: contador y vista │ navegación │ acciones y vistas.
// Si no cabe en el ancho de la terminal se descartan primero las teclas menos importantes.
func (m model) footerLine(nav []keyHint) string {
	n := len(m.filtered)
	total := len(m.contexts)
	if m.collapsible() && m.headersActive() && m.search == "" {
		n = total // con grupos plegados len(filtered) no refleja el total
	}
	counter := counterStyle.Render(fmt.Sprintf("%d/%d", n, total)) + " " + hintKeySt.Render(m.ev())

	navGroup := append([]keyHint{{"↑↓", "move", 0}, {"⏎", "switch", 0}}, nav...)
	hLabel := "full"
	if !m.shortNames {
		hLabel = "short"
	}
	navGroup = append(navGroup, keyHint{"^h", hLabel, 1})
	actions := []keyHint{{"^p", "pin", 1}, {"^f", "pinned", 2}, {"^t", "jump pin", 3}}
	system := []keyHint{{"^v", "views", 0}, {"esc", "quit", 0}}

	render := func(hs []keyHint, maxPrio int) string {
		var out []string
		for _, h := range hs {
			if h.prio <= maxPrio {
				out = append(out, hintKeySt.Render(h.key)+" "+hintLabelSt.Render(h.label))
			}
		}
		return strings.Join(out, "  ")
	}
	sep := " " + dimStyle.Render("│") + " "
	build := func(maxPrio int) string {
		blocks := []string{counter}
		for _, g := range [][]keyHint{navGroup, actions, system} {
			if s := render(g, maxPrio); s != "" {
				blocks = append(blocks, s)
			}
		}
		return "  " + "  " + strings.Join(blocks, sep)
	}
	for p := 3; p >= 0; p-- {
		if line := build(p); lipgloss.Width(line) <= m.terminalWidth-2 {
			return line
		}
	}
	return build(0)
}

// finish une cabecera, cuerpo y pie. El pie queda siempre pegado al fondo de la terminal, aunque haya
// pocos resultados o ninguno; encima lleva un separador para ordenar visualmente la pantalla.
func (m model) finish(head, body []string, nav []keyHint) string {
	if len(m.filtered) == 0 {
		body = []string{"", "  " + dimStyle.Render("  No matching contexts")}
	}
	lines := append(append([]string(nil), head...), body...)
	tail := []string{
		"  " + dimStyle.Render(strings.Repeat("─", max(10, m.terminalWidth-6))),
		m.footerLine(nav),
	}
	// Rellenar hasta que el pie ocupe la última fila de la terminal. Sin "\n" final: un salto de línea
	// al cierre agregaría una fila vacía debajo del pie.
	if pad := m.terminalHeight - len(lines) - len(tail); pad > 0 {
		lines = append(lines, make([]string, pad)...)
	}
	lines = append(lines, tail...)
	return strings.Join(lines, "\n")
}

// ── columns / grouped / tree / region ──────────────────

func (m model) viewTable() string {
	showProfile := true
	if m.headersActive() && (m.ev() == viewGrouped || m.ev() == viewTree) {
		showProfile = false // el encabezado del grupo ya muestra el profile
	}
	c := m.colLayoutFor(m.terminalWidth, showProfile)
	nav := []keyHint{{"←→", "tabs", 1}}
	if m.ev() == viewTree || m.ev() == viewRegion {
		nav = append(nav, keyHint{"space", "fold", 1})
	}
	return m.finish(m.headLines(true), m.bodyRows(c), nav)
}

// ── kubectx: lista estilo fzf ───────────────────────────

var (
	kxCurrentFg = lipgloss.Color("#50fa7b")
	// Filas fuera del cursor atenuadas ("opacidad"): así la fila seleccionada resalta al recorrer la lista
	kxDimFg     = lipgloss.Color("#7d828e")
	kxDimCurFg  = lipgloss.Color("#3fa66a") // contexto actual atenuado (sigue siendo verde, pero suave)
	kxDimPinFg  = lipgloss.Color("#a3a367")
	kxDimAlias  = lipgloss.Color("#8f77b8")
	kxCursorBg  = lipgloss.Color("#2f3441") // franja suave bajo la fila del cursor (sin cambiar el color del texto)
	kxPointerFg = lipgloss.Color("#00d4ff")
	kxCounterSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#b5bd68"))
	kxPromptSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#61afef"))
	kxQuerySt   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f8f8f2"))
	kxHintSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
	kxLineSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
)

// viewKubectx imita la interfaz de kubectx (fzf): nombres completos, el primer resultado abajo junto al prompt,
// el contexto actual en verde y el cursor resaltado. Solo quedan el contador y el prompt en la parte inferior.
func (m model) viewKubectx() string {
	w := m.terminalWidth
	rows := m.maxVisible()
	n := len(m.filtered)
	start := m.scrollOffset
	if start > n {
		start = 0
	}

	lines := make([]string, 0, rows+2)
	// De arriba hacia abajo se dibuja el índice más alto hasta el más bajo; los huecos quedan arriba
	for r := rows - 1; r >= 0; r-- {
		pos := start + r
		if pos >= n {
			lines = append(lines, "")
			continue
		}
		idx := m.filtered[pos]
		ctx := m.contexts[idx]
		selected := pos == m.cursor

		// Igual que en las demás vistas: la fila del cursor va siempre en el mismo azul (#00d4ff, negrita) sin
		// importar qué cluster sea, más una franja de fondo suave; el resto va en color normal y solo el
		// contexto actual en verde.
		base := lipgloss.NewStyle()
		pinFg, aliasFg := kxDimPinFg, kxDimAlias
		var nameSt lipgloss.Style
		switch {
		case selected:
			base = base.Background(kxCursorBg)
			nameSt = base.Bold(true).Foreground(kxPointerFg)
			pinFg, aliasFg = lipgloss.Color("#f1fa8c"), lipgloss.Color("#bd93f9")
		case ctx == m.current:
			nameSt = base.Foreground(kxDimCurFg)
		default:
			nameSt = base.Foreground(kxDimFg)
		}
		suffix := ""
		if m.isPinned(ctx) {
			suffix += base.Foreground(pinFg).Render(" ★")
		}
		if alias := m.aliasFor(ctx); alias != "" {
			suffix += base.Bold(selected).Foreground(aliasFg).Render(" @" + alias)
		}
		maxW := max(10, w-6-lipgloss.Width(suffix))
		name := truncateCell(m.nameOf(idx), maxW)
		if !m.shortNames {
			name = truncateLeft(m.nameOf(idx), maxW)
		}
		pointer := base.Render("  ")
		if selected {
			pointer = base.Bold(true).Foreground(kxPointerFg).Render("> ")
		}
		line := pointer + nameSt.Render(name) + suffix
		if selected {
			// extender la franja hasta el borde derecho
			line += base.Render(strings.Repeat(" ", max(0, w-lipgloss.Width(line))))
		}
		lines = append(lines, line)
	}

	// Contador + línea fina (con el atajo para cambiar de vista) y prompt
	counter := kxCounterSt.Render(fmt.Sprintf("  %d/%d", n, len(m.contexts)))
	hint := kxHintSt.Render(" ^h names · ^v views ")
	fill := max(3, w-lipgloss.Width(counter)-lipgloss.Width(hint)-3)
	lines = append(lines, counter+" "+kxLineSt.Render(strings.Repeat("─", fill))+hint)
	lines = append(lines, kxPromptSt.Render("> ")+kxQuerySt.Render(m.search)+kxPromptSt.Render("█"))
	return strings.Join(lines, "\n")
}

// ── simple: lista sencilla con ambiente y profile ──────

func (m model) viewSimple() string {
	c := m.colLayoutFor(m.terminalWidth, true)
	c.showRegion = false
	rows := m.displayRows()
	start, end := m.window(len(rows))
	var body []string
	if start > 0 {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▲ %d more", start)))
	}
	for _, r := range rows[start:end] {
		body = append(body, m.itemLine(c, r.pos, ""))
	}
	if end < len(rows) {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▼ %d more", len(rows)-end)))
	}
	return m.finish(m.headLines(true), body, nil)
}

// ── split: lista + panel de detalle ────────────────────

func (m model) viewSplit() string {
	w := m.terminalWidth
	c := m.colLayoutFor(min(w*50/100, 64), false)
	// ancho real de la lista: sangría+puntero + cluster + env + etiquetas (●★⏱@alias)
	leftW := min(5+c.clusterW+tableGap+c.envW+16, w*55/100)
	rightW := w - leftW - 8
	left := m.bodyRows(c)
	right := m.panelLines(rightW)

	n := max(len(left), len(right))
	body := make([]string, 0, n)
	sep := dimStyle.Render(" │ ")
	for i := 0; i < n; i++ {
		l, r := "", ""
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		body = append(body, padRight(l, leftW+2)+sep+r)
	}
	return m.finish(m.headLines(false), body, []keyHint{{"←→", "tabs", 1}})
}

// panelLines es la ficha del cluster seleccionado.
func (m model) panelLines(w int) []string {
	if len(m.filtered) == 0 {
		return nil
	}
	cur := min(m.cursor, len(m.filtered)-1)
	idx := m.filtered[cur]
	ctx := m.contexts[idx]
	in := m.infos[idx]
	row := func(label, value string) string {
		return columnHeadSt.Render(fmt.Sprintf("%-8s", label)) + " " + value
	}
	lines := []string{"", groupHeadSt.Render(truncateCell(in.Cluster, w)), ""}
	env := "—"
	if in.Env != "" {
		env = envStyle(in.Env).Render(strings.ToUpper(in.Env))
	}
	lines = append(lines, row("env", env))
	if in.EKS {
		profile := in.Profile
		if profile == "" {
			profile = dimStyle.Render("no profile")
		}
		lines = append(lines, row("profile", truncateCell(profile, w-9)), row("account", in.Account), row("region", in.Region))
	} else {
		lines = append(lines, row("type", "kubeconfig context"))
	}
	if alias := m.aliasFor(ctx); alias != "" {
		lines = append(lines, row("alias", aliasStyle.Render("@"+alias)))
	}
	state := []string{}
	if ctx == m.current {
		state = append(state, activeTag+" current")
	}
	if m.isPinned(ctx) {
		state = append(state, pinTag+" pinned")
	}
	if rank, ok := m.recent[ctx]; ok && rank > 0 {
		state = append(state, recentTag+fmt.Sprintf(" recent #%d", rank))
	}
	if len(state) > 0 {
		lines = append(lines, row("state", strings.Join(state, "  ")))
	}
	var groups []string
	for g, members := range m.cfg.Groups {
		for _, mem := range members {
			if mem == ctx {
				groups = append(groups, g)
			}
		}
	}
	if len(groups) > 0 {
		lines = append(lines, row("groups", truncateCell(strings.Join(groups, ", "), w-9)))
	}
	lines = append(lines, "")
	for _, l := range wrapText(ctx, w) {
		lines = append(lines, dimStyle.Render(l))
	}
	return lines
}

// ── matrix: aplicaciones × ambientes ───────────────────

const matrixCellW = 7

func (m model) viewMatrix() string {
	cols, rows := m.matrixLayout()
	maxApp := 8
	for _, r := range rows {
		if l := lipgloss.Width(r.label); l > maxApp {
			maxApp = l
		}
	}
	appW := min(maxApp, max(12, m.terminalWidth-8-matrixCellW*len(cols)))

	hdr := "  " + "   " + padStyled("APP", appW, columnHeadSt)
	for _, e := range cols {
		label := strings.ToUpper(e)
		st := envStyle(e)
		if e == "" {
			label, st = "—", dimStyle
		}
		hdr += padStyled(centerText(label, matrixCellW), matrixCellW, st)
	}
	body := []string{hdr}

	start, end := m.window(len(rows))
	if start > 0 {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▲ %d more", start)))
	}
	for _, r := range rows[start:end] {
		hasCursor := false
		for _, pos := range r.cells {
			if pos == m.cursor {
				hasCursor = true
			}
		}
		pointer, nameSt := "   ", normalItemStyle
		if hasCursor {
			pointer, nameSt = " ❯ ", selectedItemStyle
		}
		line := "  " + pointer + padName(r.label, appW, nameSt, !m.shortNames)
		for ci, pos := range r.cells {
			line += m.matrixCell(pos, cols[ci])
		}
		body = append(body, line)
	}
	if end < len(rows) {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▼ %d more", len(rows)-end)))
	}
	body = append(body, "  "+dimStyle.Render("    ● cluster   ◉ current   ★ pinned   [ ] selected"))
	return m.finish(m.headLines(true), body, []keyHint{{"←→", "env", 1}})
}

func centerText(s string, w int) string {
	pad := w - lipgloss.Width(s)
	if pad <= 0 {
		return s
	}
	left := pad / 2
	return strings.Repeat(" ", left) + s
}

// matrixCell dibuja una celda de la matriz en matrixCellW columnas visibles.
func (m model) matrixCell(pos int, env string) string {
	token := dimStyle.Render("·")
	if pos >= 0 {
		ctx := m.contexts[m.filtered[pos]]
		glyph, st := "●", envStyle(env)
		switch {
		case ctx == m.current:
			glyph, st = "◉", activeItemStyle
		case m.isPinned(ctx):
			glyph, st = "★", pinItemStyle
		}
		if pos == m.cursor {
			token = selectedItemStyle.Render("[") + st.Render(glyph) + selectedItemStyle.Render("]")
		} else {
			token = " " + st.Render(glyph) + " "
		}
	} else {
		token = " " + token + " "
	}
	return padRight("  "+token, matrixCellW)
}

// ── cards: una tarjeta por aplicación con todos sus ambientes ──

// cardLine es una línea de pantalla de la vista de tarjetas; pos es la posición en m.filtered (-1 si es decorativa).
type cardLine struct {
	text   string
	pos    int
	card   int
	header bool // primera línea (borde superior) de una tarjeta
}

var (
	cardBorderSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#44475a"))
	cardBorderSelSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#00d4ff"))
)

// buildCards arma todas las líneas de todas las tarjetas a partir de m.filtered (ordenado por app y ambiente).
// Cada tarjeta muestra los ambientes que existen en el kubeconfig; los que faltan aparecen atenuados
// ("none") salvo que haya búsqueda o filtros activos, donde solo se ven las coincidencias.
func (m model) buildCards() []cardLine {
	envs, _ := m.presentEnvs()
	showMissing := m.search == "" && !m.showPinnedOnly && m.activeGroup == ""

	// Anchos de columna comunes a todas las tarjetas
	// (se calculan sobre todos los contextos para que las tarjetas no cambien de ancho mientras se busca)
	clusterW, profileW := 8, 0
	for i, in := range m.infos {
		clusterW = max(clusterW, lipgloss.Width(m.nameOf(i)))
		profileW = max(profileW, lipgloss.Width(in.Profile))
	}
	nameCap := 44
	if !m.shortNames {
		nameCap = 70
	}
	clusterW, profileW = min(clusterW, nameCap), min(profileW, 28)
	const tagW = 10
	need := 2 + 4 + 1 + clusterW + 2 + profileW + tagW
	innerW := max(50, min(need, min(m.terminalWidth-8, 140)))
	// el nombre se recorta para que la fila nunca desborde la tarjeta (relevante con nombres completos)
	clusterW = max(12, min(clusterW, innerW-(2+4+1+2+profileW+tagW)))

	// Agrupar posiciones consecutivas por aplicación
	type group struct {
		app   string
		first int // primer contexto del grupo (para la etiqueta con nombre completo)
		pos   []int
		byEn  map[string][]int
		noEn  []int
	}
	var groups []*group
	for pos, idx := range m.filtered {
		in := m.infos[idx]
		app := baseName(in)
		if len(groups) == 0 || groups[len(groups)-1].app != app {
			groups = append(groups, &group{app: app, first: idx, byEn: map[string][]int{}})
		}
		g := groups[len(groups)-1]
		g.pos = append(g.pos, pos)
		if in.Env == "" {
			g.noEn = append(g.noEn, pos)
		} else {
			g.byEn[in.Env] = append(g.byEn[in.Env], pos)
		}
	}

	row := func(pos int) string {
		idx := m.filtered[pos]
		in, ctx := m.infos[idx], m.contexts[idx]
		selected := pos == m.cursor
		pointer := "  "
		if selected {
			pointer = selectedItemStyle.Render("❯ ")
		}
		return padRight(pointer+envBadge(in.Env, 4)+" "+padName(m.nameOf(idx), clusterW, m.nameStyle(ctx, selected), !m.shortNames)+
			"  "+padStyled(in.Profile, profileW, dimStyle)+m.tagsFor(ctx), innerW)
	}

	var out []cardLine
	for gi, g := range groups {
		hasCursor := false
		for _, p := range g.pos {
			if p == m.cursor {
				hasCursor = true
			}
		}
		bs := cardBorderSt
		if hasCursor {
			bs = cardBorderSelSt
		}
		title := strings.TrimPrefix(g.app, "eks-")
		if title == "" {
			title = g.app
		}
		if !m.shortNames {
			title = m.fullBase(g.first) // con nombres completos el título también es el ARN sin sufijo de ambiente
		}
		// Puntos de estado: un punto por ambiente, atenuado si la app no lo tiene
		var dots []string
		for _, e := range envs {
			if len(g.byEn[e]) > 0 {
				dots = append(dots, envStyle(e).Render("●"))
			} else {
				dots = append(dots, dimStyle.Render("○"))
			}
		}
		hasEnvItem := len(g.pos) > len(g.noEn)
		dotsStr := ""
		if hasEnvItem { // las apps sin ambiente (minikube, liftie...) no llevan puntos
			dotsStr = strings.Join(dots, " ")
		}
		titleStr := groupHeadSt.Render(truncateCell(title, innerW-lipgloss.Width(dotsStr)-8))
		var top string
		if dotsStr != "" {
			fill := max(1, innerW-3-lipgloss.Width(titleStr)-lipgloss.Width(dotsStr))
			top = bs.Render("╭─ ") + titleStr + bs.Render(" "+strings.Repeat("─", fill)+" ") + dotsStr + bs.Render(" ╮")
		} else {
			fill := max(1, innerW-lipgloss.Width(titleStr)-1)
			top = bs.Render("╭─ ") + titleStr + bs.Render(" "+strings.Repeat("─", fill)+"╮")
		}
		out = append(out, cardLine{text: "  " + top, pos: -1, card: gi, header: true})

		side := bs.Render("│")
		emit := func(content string, pos int) {
			out = append(out, cardLine{text: "  " + side + " " + content + " " + side, pos: pos, card: gi})
		}
		for _, e := range envs {
			if items := g.byEn[e]; len(items) > 0 {
				for _, p := range items {
					emit(row(p), p)
				}
			} else if showMissing && hasEnvItem {
				emit(padRight("  "+padStyled(strings.ToUpper(e), 4, dimStyle)+" "+dimStyle.Render("· none"), innerW), -1)
			}
		}
		for _, p := range g.noEn {
			emit(row(p), p)
		}
		out = append(out, cardLine{text: "  " + bs.Render("╰"+strings.Repeat("─", innerW+2)+"╯"), pos: -1, card: gi})
	}
	return out
}

func (m model) viewCards() string {
	lines := m.buildCards()
	start, end := m.window(len(lines))
	countCards := func(ls []cardLine) int {
		n := 0
		for _, l := range ls {
			if l.header {
				n++
			}
		}
		return n
	}
	var body []string
	if n := countCards(lines[:start]); start > 0 && n > 0 {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▲ %s above", plural(n, "card", "cards"))))
	}
	for _, l := range lines[start:end] {
		body = append(body, l.text)
	}
	if n := countCards(lines[end:]); end < len(lines) && n > 0 {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    ▼ %s below", plural(n, "card", "cards"))))
	}
	return m.finish(m.headLines(true), body, []keyHint{{"←→", "card", 1}})
}

// ── grid: nombres en varias columnas ───────────────────

func (m model) viewGrid() string {
	colW, visCols, rows := m.gridGeometry()
	nameW := colW - 5
	n := len(m.filtered)
	var body []string
	for r := 0; r < rows; r++ {
		var line strings.Builder
		line.WriteString("  ")
		has := false
		for c := 0; c < visCols; c++ {
			pos := (m.scrollOffset+c)*rows + r
			if pos >= n {
				line.WriteString(strings.Repeat(" ", colW))
				continue
			}
			has = true
			idx := m.filtered[pos]
			ctx := m.contexts[idx]
			in := m.infos[idx]
			pointer := "  "
			selected := pos == m.cursor
			if selected {
				pointer = "❯ "
			}
			dot := envStyle(in.Env).Render("●")
			if in.Env == "" {
				dot = dimStyle.Render("●")
			}
			cell := selectedItemStyle.Render(pointer) + dot + " " + padName(m.nameOf(idx), nameW, m.nameStyle(ctx, selected), !m.shortNames) + " "
			line.WriteString(padRight(cell, colW))
		}
		if !has && r > 0 {
			break
		}
		body = append(body, strings.TrimRight(line.String(), " "))
	}
	totalCols := (n + rows - 1) / rows
	if totalCols > visCols {
		body = append(body, "  "+dimStyle.Render(fmt.Sprintf("    columns %d–%d of %d  ←→", m.scrollOffset+1, min(m.scrollOffset+visCols, totalCols), totalCols)))
	}
	return m.finish(m.headLines(true), body, []keyHint{{"←→", "column", 1}, {"tab", "tabs", 1}})
}

// ── selector de vistas (Ctrl+V) ────────────────────────

func (m model) viewPicker() string {
	var b strings.Builder
	b.WriteString("  " + tabActiveSt.Render("Views") + "  " + dimStyle.Render("↑↓ select · enter apply · ctrl+v next · esc cancel") + "\n")
	b.WriteString("  " + dimStyle.Render(strings.Repeat("─", max(10, m.terminalWidth-6))) + "\n\n")
	for i, s := range viewSpecs {
		pointer, nameSt := "   ", normalItemStyle
		if i == m.pickerCur {
			pointer, nameSt = " ❯ ", selectedItemStyle
		}
		line := "  " + pointer + padStyled(s.name, 9, nameSt) + " " + dimStyle.Render(s.desc)
		if s.name == m.view {
			line += "  " + activeTag + dimStyle.Render(" current")
		}
		b.WriteString(line + "\n")
	}
	b.WriteString("\n  " + dimStyle.Render("Your choice is remembered in ~/.ksw.json (view_mode). Narrow terminals (<90 cols) always use the list.") + "\n")
	return b.String()
}
