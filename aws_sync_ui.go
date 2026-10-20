package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// ── Paleta compartida por los reportes de sync (CLI y TUI) ──

var (
	rpOk      = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b"))
	rpOkText  = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	rpAccent  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
	rpPurple  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#bd93f9"))
	rpWarn    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#ffb86c"))
	rpWarnTxt = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	rpErr     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#ff5555"))
	rpErrText = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	rpDim     = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	rpText    = lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	rpBorder  = lipgloss.NewStyle().Foreground(lipgloss.Color("#44475a"))
)

// ── Tabla con bordes redondeados ───────────────────────

type tableCol struct {
	Header string
	Style  lipgloss.Style
}

// truncateCell recorta s a w columnas visibles agregando "…".
func truncateCell(s string, w int) string {
	if w <= 0 || lipgloss.Width(s) <= w {
		return s
	}
	r := []rune(s)
	for len(r) > 0 && lipgloss.Width(string(r))+1 > w {
		r = r[:len(r)-1]
	}
	return string(r) + "…"
}

// renderTable dibuja una tabla con bordes redondeados que no excede maxW columnas.
// Las celdas son texto plano; el color lo define cada columna.
func renderTable(cols []tableCol, rows [][]string, accent lipgloss.Style, maxW int) []string {
	n := len(cols)
	widths := make([]int, n)
	for i, c := range cols {
		widths[i] = lipgloss.Width(c.Header)
	}
	for _, r := range rows {
		for i := 0; i < n && i < len(r); i++ {
			if w := lipgloss.Width(r[i]); w > widths[i] {
				widths[i] = w
			}
		}
	}
	// Ancho total = bordes (n+1) + padding (2 por columna) + contenido
	total := func() int {
		t := n + 1 + 2*n
		for _, w := range widths {
			t += w
		}
		return t
	}
	// Achicar la columna más ancha hasta que quepa (mínimo 8)
	for maxW > 0 && total() > maxW {
		widest := 0
		for i := range widths {
			if widths[i] > widths[widest] {
				widest = i
			}
		}
		if widths[widest] <= 8 {
			break
		}
		widths[widest]--
	}

	cell := func(text string, w int, st lipgloss.Style) string {
		text = truncateCell(text, w)
		pad := w - lipgloss.Width(text)
		if pad < 0 {
			pad = 0
		}
		return " " + st.Render(text) + strings.Repeat(" ", pad) + " "
	}
	line := func(left, mid, right string) string {
		parts := make([]string, n)
		for i, w := range widths {
			parts[i] = strings.Repeat("─", w+2)
		}
		return rpBorder.Render(left + strings.Join(parts, mid) + right)
	}
	bar := rpBorder.Render("│")

	var out []string
	out = append(out, line("╭", "┬", "╮"))
	hdr := bar
	for i, c := range cols {
		hdr += cell(c.Header, widths[i], accent) + bar
	}
	out = append(out, hdr)
	out = append(out, line("├", "┼", "┤"))
	for _, r := range rows {
		l := bar
		for i := 0; i < n; i++ {
			text := ""
			if i < len(r) {
				text = r[i]
			}
			l += cell(text, widths[i], cols[i].Style) + bar
		}
		out = append(out, l)
	}
	out = append(out, line("╰", "┴", "╯"))
	return out
}

// sectionTitle: "✚ NEW PROFILES  3"
func sectionTitle(icon string, st lipgloss.Style, title string, count int) string {
	return st.Render(icon+" "+title) + "  " + rpDim.Render(fmt.Sprintf("%d", count))
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// indentLines antepone indent a cada línea.
func indentLines(lines []string, indent string) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = indent + l
	}
	return out
}

// ── Tabla de cuentas que dejaron de existir ────────────

func staleTableLines(stale []staleAccount, maxW int) []string {
	cols := []tableCol{
		{"Profile", rpText},
		{"Account ID", rpDim},
		{"Clusters in kubeconfig", rpWarnTxt},
	}
	var rows [][]string
	for _, a := range stale {
		clusters := "—"
		if len(a.Contexts) > 0 {
			clusters = plural(len(a.Contexts), "cluster", "clusters")
		}
		rows = append(rows, []string{strings.Join(a.Profiles, ", "), a.AccountID, clusters})
	}
	return renderTable(cols, rows, rpWarn, maxW)
}

// staleSection arma el bloque "NO LONGER IN SSO" con su tabla.
// hint es el texto de ayuda que explica cómo actuar (tecla en TUI, prompt en CLI).
func staleSection(stale []staleAccount, hint string, maxW int) []string {
	var out []string
	out = append(out, sectionTitle("⚠", rpWarn, "NO LONGER IN AWS SSO", len(stale)))
	out = append(out, rpDim.Render("These accounts have a profile in ~/.aws/config but AWS SSO no longer lists them."))
	out = append(out, staleTableLines(stale, maxW)...)
	if hint != "" {
		out = append(out, hint)
	}
	return out
}

// ── Reporte: sync de profiles ──────────────────────────

// syncProfilesReport devuelve las líneas (sin indentación) del resultado de sync de profiles.
func syncProfilesReport(res *syncProfilesResult, staleHint string, maxW int) []string {
	var out []string

	summary := fmt.Sprintf("%s  %s  %s",
		rpDim.Render("session "+res.session),
		rpDim.Render(plural(res.ssoTotal, "SSO account", "SSO accounts")),
		rpDim.Render(plural(res.total, "profile", "profiles")))

	if len(res.addedProfiles) == 0 && len(res.changes) == 0 {
		out = append(out, rpOk.Render("✔ All profiles are up to date")+"  "+summary)
	} else {
		out = append(out, rpOk.Render(fmt.Sprintf("✔ Sync complete — %d added, %d fixed", res.added, res.fixed))+"  "+summary)
	}

	if len(res.addedProfiles) > 0 {
		out = append(out, "")
		out = append(out, sectionTitle("✚", rpOk, "NEW ACCOUNTS ADDED", len(res.addedProfiles)))
		added := append([]syncedProfile(nil), res.addedProfiles...)
		sort.Slice(added, func(i, j int) bool { return added[i].Name < added[j].Name })
		cols := []tableCol{
			{"Profile", rpOkText},
			{"Account ID", rpText},
			{"SSO account name", rpDim},
		}
		var rows [][]string
		for _, p := range added {
			rows = append(rows, []string{p.Name, p.AccountID, p.AccountName})
		}
		out = append(out, renderTable(cols, rows, rpAccent, maxW)...)
		out = append(out, rpDim.Render("Use: aws <command> --profile <profile>"))
	}

	if len(res.changes) > 0 {
		out = append(out, "")
		out = append(out, sectionTitle("⟳", rpPurple, "PROFILES UPDATED", len(res.changes)))
		cols := []tableCol{
			{"Profile", rpText},
			{"Change", rpPurple},
			{"Details", rpDim},
		}
		var rows [][]string
		for _, c := range res.changes {
			rows = append(rows, []string{c.Profile, c.Kind, c.Detail})
		}
		out = append(out, renderTable(cols, rows, rpAccent, maxW)...)
	}

	if len(res.skipped) > 0 {
		out = append(out, "")
		out = append(out, sectionTitle("·", rpDim, "SKIPPED", len(res.skipped)))
		for _, l := range res.skipped {
			out = append(out, "  "+rpDim.Render("· "+truncateCell(l.text, maxW-4)))
		}
	}

	if len(res.stale) > 0 {
		out = append(out, "")
		out = append(out, staleSection(res.stale, staleHint, maxW)...)
	}

	if res.backup != "" {
		out = append(out, "")
		out = append(out, rpDim.Render("Backup: "+res.backup))
	}
	return out
}

// ── Reporte: sync de kubeconfig ────────────────────────

// kubeSyncReport devuelve las líneas (sin indentación) del resultado de sync de kubeconfig.
func kubeSyncReport(res *eksSyncResultData, staleHint string, maxW int) []string {
	var out []string

	out = append(out, rpOk.Render(fmt.Sprintf("✔ Kubeconfig sync — %d added", res.added))+"  "+
		rpDim.Render(fmt.Sprintf("%d skipped  ·  %d failed", res.skipped, res.failed)))

	if len(res.addedClusters) > 0 {
		accounts := make(map[string]bool)
		newAccounts := make(map[string]bool)
		for _, c := range res.addedClusters {
			key := c.AccountID
			if key == "" {
				key = "profile:" + c.Profile
			}
			accounts[key] = true
			if c.NewAccount {
				newAccounts[key] = true
			}
		}
		out = append(out, "")
		title := sectionTitle("✚", rpOk, "NEW CLUSTERS ADDED", len(res.addedClusters))
		title += "  " + rpDim.Render("across "+plural(len(accounts), "account", "accounts"))
		if len(newAccounts) > 0 {
			title += "  " + rpOkText.Render(fmt.Sprintf("★ %d new", len(newAccounts)))
		}
		out = append(out, title)

		added := append([]eksAddedCluster(nil), res.addedClusters...)
		sort.Slice(added, func(i, j int) bool {
			if added[i].Profile != added[j].Profile {
				return added[i].Profile < added[j].Profile
			}
			return added[i].Cluster < added[j].Cluster
		})
		cols := []tableCol{
			{"Profile", rpOkText},
			{"Account ID", rpText},
			{"Cluster", rpText},
			{"Region", rpDim},
		}
		var rows [][]string
		prev := ""
		for _, c := range added {
			profile, id := c.Profile, c.AccountID
			if id == "" {
				id = "—"
			}
			if c.Profile == prev {
				profile, id = "", "" // agrupar visualmente por cuenta
			} else if c.NewAccount {
				profile = "★ " + profile
			}
			prev = c.Profile
			rows = append(rows, []string{profile, id, c.Cluster, c.Region})
		}
		out = append(out, renderTable(cols, rows, rpAccent, maxW)...)
		if len(newAccounts) > 0 {
			out = append(out, rpDim.Render("★ account had no clusters in your kubeconfig before"))
		}
	}

	// Problemas: líneas fallidas (errores de profile / cluster)
	var issues []eksSyncLine
	for _, l := range res.lines {
		if !l.ok {
			issues = append(issues, l)
		}
	}
	if len(issues) > 0 {
		out = append(out, "")
		out = append(out, sectionTitle("✗", rpErr, "ISSUES", len(issues)))
		for _, l := range issues {
			out = append(out, "  "+rpErrText.Render("✗ ")+rpDim.Render(truncateCell(l.msg, maxW-4)))
		}
	}

	if len(res.stale) > 0 {
		out = append(out, "")
		out = append(out, staleSection(res.stale, staleHint, maxW)...)
	} else if res.staleNote != "" {
		out = append(out, "")
		out = append(out, rpDim.Render("· "+res.staleNote))
	}
	return out
}

// ── Prompts interactivos para CLI ──────────────────────

func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func askLine(r *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	ans, _ := r.ReadString('\n')
	return strings.ToLower(strings.TrimSpace(ans))
}

// promptStaleRemoval ofrece eliminar los profiles (y sus clusters) de cuentas que ya no existen.
// En modo no interactivo no hace nada (el reporte ya mostró el aviso).
func promptStaleRemoval(stale []staleAccount) {
	if len(stale) == 0 || !stdinIsTerminal() {
		return
	}
	r := bufio.NewReader(os.Stdin)
	fmt.Println()
	ans := askLine(r, fmt.Sprintf("  %s Remove these accounts? %s ",
		rpWarn.Render("?"), rpDim.Render("[y] all  [s] select  [N] none:")))

	var chosen []staleAccount
	switch ans {
	case "y", "yes":
		chosen = stale
	case "s", "select":
		for _, a := range stale {
			extra := ""
			if len(a.Contexts) > 0 {
				extra = ", " + plural(len(a.Contexts), "cluster", "clusters")
			}
			a1 := askLine(r, fmt.Sprintf("    Remove %s %s? %s ",
				rpText.Render(strings.Join(a.Profiles, ", ")),
				rpDim.Render("("+a.AccountID+extra+")"),
				rpDim.Render("[y/N]")))
			if a1 == "y" || a1 == "yes" {
				chosen = append(chosen, a)
			}
		}
	}
	if len(chosen) == 0 {
		fmt.Printf("  %s Nothing removed\n", rpDim.Render("·"))
		return
	}

	withClusters := false
	if n := countStaleClusters(chosen); n > 0 {
		a2 := askLine(r, fmt.Sprintf("  %s Also remove %s from kubeconfig? %s ",
			rpWarn.Render("?"), plural(n, "cluster", "clusters"), rpDim.Render("[Y/n]")))
		withClusters = a2 == "" || a2 == "y" || a2 == "yes"
	}

	res := removeStaleAccounts(chosen, withClusters)
	fmt.Println()
	for _, l := range staleRemovalLines(res, withClusters) {
		fmt.Println("  " + l)
	}
}

// staleRemovalLines resume el resultado de una eliminación.
func staleRemovalLines(res staleRemoval, withClusters bool) []string {
	if res.Err != nil {
		return []string{rpErr.Render("✗ " + res.Err.Error())}
	}
	out := []string{rpOk.Render(fmt.Sprintf("✔ Removed %s", plural(res.Profiles, "profile", "profiles")))}
	if withClusters {
		out = append(out, rpOk.Render(fmt.Sprintf("✔ Removed %s from kubeconfig", plural(res.Clusters, "cluster", "clusters"))))
	} else {
		out = append(out, rpDim.Render("· Clusters were kept in kubeconfig"))
	}
	if res.Backup != "" {
		out = append(out, rpDim.Render("Backup: "+res.Backup))
	}
	return out
}

// printReport imprime las líneas con indentación estándar de 2 espacios.
func printReport(lines []string) {
	for _, l := range indentLines(lines, "  ") {
		fmt.Println(l)
	}
}
