package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── Tipos y resultados ─────────────────────────────────

type eksTUIStep int

const (
	eksStepMenu        eksTUIStep = iota // menú principal
	eksStepSyncing                       // spinner: sync en progreso
	eksStepSyncResult                    // resultado del sync
	eksStepProfiles                      // lista de profiles para sync individual
	eksStepViewClusters                  // clusters actuales en kubeconfig
	eksStepStale                         // clusters stale detectados
	eksStepConfirmStale                  // confirmar eliminación de stale
	eksStepConfirmClearAll               // confirmar eliminación de todos los clusters EKS
)

type eksMenuAction int

const (
	eksActionSyncAll    eksMenuAction = iota
	eksActionSyncOne                  // sync de un profile específico
	eksActionViewClusters             // ver clusters en kubeconfig
	eksActionRemoveStale              // detectar y remover clusters stale
	eksActionClearAll                 // eliminar todos los contextos EKS del kubeconfig
	eksActionQuit
)

var eksMenuLabels = []string{
	"Sync all profiles",
	"Sync specific profile",
	"View clusters in kubeconfig",
	"Remove stale clusters",
	"Clear all clusters from kubeconfig",
	"Quit",
}

// eksSyncLine es una línea de resultado del sync
type eksSyncLine struct {
	ok      bool
	cluster string
	profile string
	msg     string
}

// eksSyncResultData acumula resultados de un sync EKS
type eksSyncResultData struct {
	lines   []eksSyncLine
	added   int
	skipped int
	failed  int
	err     error
}

// eksClusterEntry representa un cluster en kubeconfig
type eksClusterEntry struct {
	context string
	exists  bool // true = aún existe en AWS, false = stale
}

// Mensajes tea
type eksSyncDoneMsg struct{ result eksSyncResultData }
type eksSyncProgressMsg struct{ line eksSyncLine }
type eksStaleCheckDoneMsg struct{ stale []string }
type eksStaleRemovedMsg struct{ removed int; failed int }
type eksClearAllDoneMsg struct{ removed int; failed int }

// drainSyncCh lee una línea del canal de progreso o el resultado final
func drainSyncCh(progressCh <-chan eksSyncLine, resultCh <-chan eksSyncResultData) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-progressCh
		if !ok {
			return eksSyncDoneMsg{result: <-resultCh}
		}
		return eksSyncProgressMsg{line: line}
	}
}

// ── Modelo ─────────────────────────────────────────────

type eksTUIModel struct {
	step         eksTUIStep
	cursor       int
	menuAction   eksMenuAction

	// profiles disponibles
	profiles    []awsProfile
	filterInput string

	// resultados sync
	syncResult    *eksSyncResultData
	progressLines []eksSyncLine             // líneas en vivo durante sync
	syncProgressCh <-chan eksSyncLine        // canal de progreso activo
	syncResultCh   <-chan eksSyncResultData  // canal de resultado final
	spinnerFrame  int

	// clusters en kubeconfig
	clusters    []eksClusterEntry
	staleClusters  []string
	allEKSContexts []string // todos los contextos EKS en kubeconfig (para clear all)

	// info general
	profileCount int
	clusterCount int

	width  int
	height int
	quitting bool
}

// ── Styles ─────────────────────────────────────────────

var (
	eksTitleSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00b4d8"))
	eksOkSt     = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	eksWarnSt   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	eksErrSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	eksDimSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	eksSelSt    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#8be9fd"))
	eksLabelSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f4a261"))
	eksNormalSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	eksBarSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#44475a"))
	eksInputSt  = lipgloss.NewStyle().Foreground(lipgloss.Color("#8be9fd"))
)

var eksSpinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func tuiProgressBar(current, total, width int) string {
	if total == 0 || width <= 0 {
		return eksDimSt.Render(strings.Repeat("░", width))
	}
	filled := current * width / total
	if filled > width {
		filled = width
	}
	cyanSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#00b4d8"))
	return cyanSt.Render(strings.Repeat("█", filled)) + eksDimSt.Render(strings.Repeat("░", width-filled))
}

// ── Init ───────────────────────────────────────────────

func (m eksTUIModel) Init() tea.Cmd {
	return tea.WindowSize()
}

// ── Commands ───────────────────────────────────────────

func eksSpinnerTick() tea.Cmd {
	return tea.Tick(80*time.Millisecond, func(_ time.Time) tea.Msg {
		return spinnerTickMsg{}
	})
}

// startEKSSync lanza el sync y retorna los canales + el primer cmd de drenaje.
func startEKSSync(profiles []awsProfile) (<-chan eksSyncLine, <-chan eksSyncResultData, tea.Cmd) {
	progressCh := make(chan eksSyncLine, 256)
	resultCh := make(chan eksSyncResultData, 1)

	go func() {
		result := runEKSSyncTUIStreaming(profiles, progressCh)
		close(progressCh)
		resultCh <- result
	}()

	return progressCh, resultCh, drainSyncCh(progressCh, resultCh)
}

func cmdCheckStale() tea.Cmd {
	return func() tea.Msg {
		stale := detectStaleClusters()
		return eksStaleCheckDoneMsg{stale: stale}
	}
}

func cmdRemoveStale(stale []string) tea.Cmd {
	return func() tea.Msg {
		removed, failed := removeStaleClusters(stale)
		return eksStaleRemovedMsg{removed: removed, failed: failed}
	}
}

func cmdClearAllEKS(contexts []string) tea.Cmd {
	return func() tea.Msg {
		removed, failed := removeStaleClusters(contexts) // misma lógica, elimina los que se pasen
		return eksClearAllDoneMsg{removed: removed, failed: failed}
	}
}

// ── Update ─────────────────────────────────────────────

func (m eksTUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case spinnerTickMsg:
		if m.step == eksStepSyncing {
			m.spinnerFrame = (m.spinnerFrame + 1) % len(eksSpinnerFrames)
			return m, eksSpinnerTick()
		}
		return m, nil

	case eksSyncProgressMsg:
		m.progressLines = append(m.progressLines, msg.line)
		return m, drainSyncCh(m.syncProgressCh, m.syncResultCh)

	case eksSyncDoneMsg:
		m.syncResult = &msg.result
		m.progressLines = nil
		m.syncProgressCh = nil
		m.syncResultCh = nil
		existing, _ := getExistingEKSContexts()
		m.clusterCount = len(existing)
		m.step = eksStepSyncResult
		return m, nil

	case eksStaleCheckDoneMsg:
		m.staleClusters = msg.stale
		m.step = eksStepStale
		return m, nil

	case eksStaleRemovedMsg:
		existing, _ := getExistingEKSContexts()
		m.clusterCount = len(existing)
		m.syncResult = &eksSyncResultData{added: msg.removed, failed: msg.failed}
		m.step = eksStepSyncResult
		return m, nil

	case eksClearAllDoneMsg:
		existing, _ := getExistingEKSContexts()
		m.clusterCount = len(existing)
		m.syncResult = &eksSyncResultData{added: msg.removed, failed: msg.failed}
		m.step = eksStepSyncResult
		return m, nil

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			// Ctrl+C siempre sale, independientemente del estado
			m.quitting = true
			return m, tea.Quit

		case tea.KeyEsc:
			if m.step == eksStepSyncing {
				return m, nil // no cancelar con Esc durante sync
			}
			switch m.step {
			case eksStepMenu:
				m.quitting = true
				return m, tea.Quit
			default:
				m.step = eksStepMenu
				m.cursor = 0
				m.filterInput = ""
				m.syncResult = nil
				m.staleClusters = nil
				m.allEKSContexts = nil
				return m, nil
			}

		default:
			if m.step == eksStepSyncing {
				return m, nil // bloquear otros inputs durante sync
			}
		}

		if m.step == eksStepSyncing {
			return m, nil
		}

		switch msg.Type {
		case tea.KeyUp:
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil

		case tea.KeyDown:
			max := m.menuLen() - 1
			if m.cursor < max {
				m.cursor++
			}
			return m, nil

		case tea.KeyEnter:
			return m.handleEnter()

		case tea.KeyBackspace:
			if m.step == eksStepProfiles && len(m.filterInput) > 0 {
				r := []rune(m.filterInput)
				m.filterInput = string(r[:len(r)-1])
				m.cursor = 0
			}
			return m, nil

		case tea.KeyRunes:
			if m.step == eksStepProfiles {
				m.filterInput += string(msg.Runes)
				m.cursor = 0
			}
			// Tecla 'y' desde confirm-stale
			if m.step == eksStepConfirmStale && string(msg.Runes) == "y" {
				m.step = eksStepSyncing
				m.spinnerFrame = 0
				return m, tea.Batch(eksSpinnerTick(), cmdRemoveStale(m.staleClusters))
			}
			// Tecla 'y' desde confirm-clear-all
			if m.step == eksStepConfirmClearAll && string(msg.Runes) == "y" {
				m.step = eksStepSyncing
				m.spinnerFrame = 0
				return m, tea.Batch(eksSpinnerTick(), cmdClearAllEKS(m.allEKSContexts))
			}
			return m, nil
		}
	}
	return m, nil
}

func (m eksTUIModel) menuLen() int {
	switch m.step {
	case eksStepMenu:
		return len(eksMenuLabels)
	case eksStepProfiles:
		return len(m.filteredProfiles())
	case eksStepStale:
		return 1 // solo el botón de confirmar
	}
	return 0
}

func (m eksTUIModel) filteredProfiles() []awsProfile {
	if m.filterInput == "" {
		return m.profiles
	}
	var out []awsProfile
	term := strings.ToLower(m.filterInput)
	for _, p := range m.profiles {
		if strings.Contains(strings.ToLower(p.Name), term) {
			out = append(out, p)
		}
	}
	return out
}

func (m eksTUIModel) handleEnter() (tea.Model, tea.Cmd) {
	switch m.step {
	case eksStepMenu:
		action := eksMenuAction(m.cursor)
		m.menuAction = action
		switch action {
		case eksActionSyncAll:
			if len(m.profiles) == 0 {
				m.syncResult = &eksSyncResultData{err: fmt.Errorf("no AWS profiles found — run 'ksw aws sso config' → Sync profiles first")}
				m.step = eksStepSyncResult
				return m, nil
			}
			m.step = eksStepSyncing
			m.spinnerFrame = 0
			m.progressLines = nil
			pCh, rCh, drainCmd := startEKSSync(m.profiles)
			m.syncProgressCh = pCh
			m.syncResultCh = rCh
			return m, tea.Batch(eksSpinnerTick(), drainCmd)

		case eksActionSyncOne:
			if len(m.profiles) == 0 {
				m.syncResult = &eksSyncResultData{err: fmt.Errorf("no AWS profiles found — run 'ksw aws sso config' → Sync profiles first")}
				m.step = eksStepSyncResult
				return m, nil
			}
			m.step = eksStepProfiles
			m.cursor = 0
			m.filterInput = ""
			return m, nil

		case eksActionViewClusters:
			existing, _ := getExistingEKSContexts()
			m.clusters = nil
			for ctx := range existing {
				m.clusters = append(m.clusters, eksClusterEntry{context: ctx, exists: true})
			}
			m.step = eksStepViewClusters
			m.cursor = 0
			return m, nil

		case eksActionRemoveStale:
			m.step = eksStepSyncing
			m.spinnerFrame = 0
			return m, tea.Batch(eksSpinnerTick(), cmdCheckStale())

		case eksActionClearAll:
			existing, _ := getExistingEKSContexts()
			m.allEKSContexts = nil
			for ctx := range existing {
				m.allEKSContexts = append(m.allEKSContexts, ctx)
			}
			if len(m.allEKSContexts) == 0 {
				m.syncResult = &eksSyncResultData{}
				m.step = eksStepSyncResult
				return m, nil
			}
			m.step = eksStepConfirmClearAll
			return m, nil

		case eksActionQuit:
			m.quitting = true
			return m, tea.Quit
		}

	case eksStepProfiles:
		filtered := m.filteredProfiles()
		if len(filtered) == 0 {
			return m, nil
		}
		selected := filtered[m.cursor]
		m.step = eksStepSyncing
		m.spinnerFrame = 0
		m.progressLines = nil
		pCh, rCh, drainCmd := startEKSSync([]awsProfile{selected})
		m.syncProgressCh = pCh
		m.syncResultCh = rCh
		return m, tea.Batch(eksSpinnerTick(), drainCmd)

	case eksStepSyncResult:
		m.step = eksStepMenu
		m.cursor = 0
		m.syncResult = nil
		return m, nil

	case eksStepViewClusters:
		m.step = eksStepMenu
		m.cursor = 0
		return m, nil

	case eksStepStale:
		if len(m.staleClusters) == 0 {
			m.step = eksStepMenu
			m.cursor = 0
			return m, nil
		}
		m.step = eksStepConfirmStale
		return m, nil

	case eksStepConfirmStale:
		// Enter sin 'y' = cancelar
		m.step = eksStepMenu
		m.cursor = 0
		m.staleClusters = nil
		return m, nil

	case eksStepConfirmClearAll:
		// Enter sin 'y' = cancelar
		m.step = eksStepMenu
		m.cursor = 0
		m.allEKSContexts = nil
		return m, nil
	}
	return m, nil
}

// ── View ───────────────────────────────────────────────

func (m eksTUIModel) View() string {
	if m.quitting || m.width == 0 {
		return ""
	}

	innerW := m.width - 4
	if innerW < 40 {
		innerW = 40
	}

	topBar := "  " + eksBarSt.Render(strings.Repeat("─", innerW-2))
	bottomBar := "  " + eksBarSt.Render(strings.Repeat("─", innerW-2))
	header := "  " + eksTitleSt.Render("⎈ ksw eks")
	if isLicenseValid() {
		premiumSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f4a261"))
		header += "  " + premiumSt.Render("★ premium")
	}

	var lines []string

	switch m.step {
	case eksStepMenu:
		// Stats
		profileCountStr := eksNormalSt.Render(fmt.Sprintf("%d", m.profileCount))
		if m.profileCount == 0 {
			profileCountStr = eksErrSt.Render("0")
		}
		lines = append(lines, "  "+eksLabelSt.Render("AWS Profiles")+
			"  "+profileCountStr+
			"   "+eksLabelSt.Render("EKS in kubeconfig")+
			"  "+eksNormalSt.Render(fmt.Sprintf("%d", m.clusterCount)))
		if m.profileCount == 0 {
			lines = append(lines, "")
			lines = append(lines, "  "+eksWarnSt.Render("⚠  No AWS profiles found in ~/.aws/config"))
			lines = append(lines, "  "+eksDimSt.Render("   Run 'ksw aws sso config' → Sync profiles first"))
		}
		lines = append(lines, "")
		lines = append(lines, "  "+eksBarSt.Render(strings.Repeat("─", innerW-2)))
		lines = append(lines, "")
		lines = append(lines, "  "+eksLabelSt.Render("Options")+"  "+eksDimSt.Render("↑↓ navigate · enter select · esc quit"))
		lines = append(lines, "")
		for i, l := range eksMenuLabels {
			if i == m.cursor {
				lines = append(lines, "  "+eksSelSt.Render("❯ "+l))
			} else {
				lines = append(lines, "    "+eksNormalSt.Render(l))
			}
		}

	case eksStepProfiles:
		// Estilos idénticos a ksw main.go
		kswSearchActive      := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f1fa8c"))
		kswSearchPlaceholder := lipgloss.NewStyle().Foreground(lipgloss.Color("#555")).Italic(true)
		kswCurrentLabel      := lipgloss.NewStyle().Foreground(lipgloss.Color("#888"))
		kswCurrentValue      := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b"))
		kswSelected          := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
		kswNormal            := lipgloss.NewStyle().Foreground(lipgloss.Color("#999"))
		kswDim               := lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
		kswCounter           := lipgloss.NewStyle().Foreground(lipgloss.Color("#666"))

		filtered := m.filteredProfiles()

		// ── Current context (igual que ksw) ──
		currentCtx := ""
		if out, err := exec.Command("kubectl", "config", "current-context").Output(); err == nil {
			currentCtx = strings.TrimSpace(string(out))
		}
		if currentCtx != "" {
			lines = append(lines, "  "+kswCurrentLabel.Render("  current ")+kswCurrentValue.Render(currentCtx))
		}
		lines = append(lines, "")

		// ── Search bar (igual que ksw) ──
		if m.filterInput != "" {
			lines = append(lines, "  "+kswSearchActive.Render("  ❯ "+m.filterInput+"█"))
		} else {
			lines = append(lines, "  "+kswSearchPlaceholder.Render("  ❯ type to search..."))
		}

		// ── Separator ──
		lines = append(lines, "  "+kswDim.Render("  ─────────────────────────────────────────"))

		if len(filtered) == 0 && len(m.profiles) == 0 {
			lines = append(lines, "")
			lines = append(lines, "  "+eksWarnSt.Render("  ⚠  No AWS profiles found in ~/.aws/config"))
			lines = append(lines, "  "+kswDim.Render("     Run 'ksw aws sso config' → Sync profiles first"))
		} else if len(filtered) == 0 {
			lines = append(lines, "")
			lines = append(lines, "  "+kswDim.Render("  No matching profiles"))
		} else {
			// current(1) + blank(1) + search(1) + sep(1) + blank(1) + footer(1) + header(1) + topBar(1) = 8
			maxVisible := m.height - 8
			if maxVisible < 5 {
				maxVisible = 5
			}

			start := 0
			if m.cursor >= start+maxVisible {
				start = m.cursor - maxVisible + 1
			}
			end := start + maxVisible
			if end > len(filtered) {
				end = len(filtered)
			}

			// Scroll arriba
			if start > 0 {
				lines = append(lines, "  "+kswDim.Render(fmt.Sprintf("    ▲ %d more", start)))
			}

			for i := start; i < end; i++ {
				p := filtered[i]
				region := kswDim.Render("  "+p.Region)
				if i == m.cursor {
					lines = append(lines, "   ❯ "+kswSelected.Render(p.Name)+region)
				} else {
					lines = append(lines, "     "+kswNormal.Render(p.Name)+region)
				}
			}

			// Scroll abajo
			if end < len(filtered) {
				lines = append(lines, "  "+kswDim.Render(fmt.Sprintf("    ▼ %d more", len(filtered)-end)))
			}
		}

		// ── Footer (igual que ksw) ──
		// Rellenar con vacíos hasta el fondo
		needed := m.height - 3 - len(lines)
		for i := 0; i < needed; i++ {
			lines = append(lines, "")
		}
		counter := kswCounter.Render(fmt.Sprintf("  %d/%d", len(filtered), len(m.profiles)))
		helpTxt := "  ↑↓ navigate · enter sync · esc back"
		lines = append(lines, "  "+counter+kswDim.Render(helpTxt))

		var bProfiles strings.Builder
		bProfiles.WriteString(header + "\n")
		bProfiles.WriteString(topBar + "\n")
		for _, l := range lines {
			bProfiles.WriteString(l + "\n")
		}
		return bProfiles.String()

	case eksStepSyncing:
		spinner := eksSpinnerFrames[m.spinnerFrame]
		switch m.menuAction {
		case eksActionRemoveStale:
			lines = append(lines, "  "+eksOkSt.Render(spinner)+" "+eksDimSt.Render("Checking for stale clusters..."))
			lines = append(lines, "")
			lines = append(lines, "  "+eksDimSt.Render("Comparing kubeconfig contexts with AWS EKS..."))
		case eksActionClearAll:
			lines = append(lines, "  "+eksOkSt.Render(spinner)+" "+eksDimSt.Render("Removing all EKS clusters from kubeconfig..."))
		
		default:
			lines = append(lines, "  "+eksOkSt.Render(spinner)+" "+eksDimSt.Render("Syncing clusters..."))
			lines = append(lines, "")
			// Usar casi toda la pantalla para las líneas de progreso
			maxLines := m.height - 8
			if maxLines < 4 {
				maxLines = 4
			}
			shown := m.progressLines
			if len(shown) > maxLines {
				shown = shown[len(shown)-maxLines:]
			}
			for _, l := range shown {
				if l.ok {
					lines = append(lines, "    "+eksOkSt.Render("✔")+" "+eksDimSt.Render(l.msg))
				} else {
					lines = append(lines, "    "+eksErrSt.Render("✗")+" "+eksDimSt.Render(l.msg))
				}
			}
			if len(m.progressLines) == 0 {
				lines = append(lines, "  "+eksDimSt.Render("Querying AWS..."))
			} else {
				barWidth := 24
				processed := len(m.progressLines)
				bar := tuiProgressBar(processed, m.profileCount, barWidth)
				lines = append(lines, "")
				lines = append(lines, fmt.Sprintf("  %s  %s",
					bar,
					eksDimSt.Render(fmt.Sprintf("%d / %d  ·  ctrl+c to cancel", processed, m.profileCount))))
			}
		}

	case eksStepSyncResult:
		if m.syncResult == nil {
			lines = append(lines, "  "+eksDimSt.Render("No result"))
		} else if m.syncResult.err != nil {
			lines = append(lines, "  "+eksErrSt.Render("✗ "+m.syncResult.err.Error()))
		} else if m.menuAction == eksActionRemoveStale {
			if m.syncResult.added == 0 {
				lines = append(lines, "  "+eksOkSt.Render("✔ No stale clusters removed"))
			} else {
				lines = append(lines, "  "+eksOkSt.Render(fmt.Sprintf("✔ Removed %d stale cluster(s)", m.syncResult.added)))
				if m.syncResult.failed > 0 {
					lines = append(lines, "  "+eksWarnSt.Render(fmt.Sprintf("  %d failed to remove", m.syncResult.failed)))
				}
			}
		} else if m.menuAction == eksActionClearAll {
			if m.syncResult.added == 0 {
				lines = append(lines, "  "+eksOkSt.Render("✔ Kubeconfig already had no EKS clusters"))
			} else {
				lines = append(lines, "  "+eksOkSt.Render(fmt.Sprintf("✔ Removed %d EKS cluster(s) from kubeconfig", m.syncResult.added)))
				if m.syncResult.failed > 0 {
					lines = append(lines, "  "+eksWarnSt.Render(fmt.Sprintf("  %d failed to remove", m.syncResult.failed)))
				}
			}
		} else {
			lines = append(lines, "  "+eksOkSt.Render(fmt.Sprintf("✔ Sync complete — %d added, %d skipped, %d failed",
				m.syncResult.added, m.syncResult.skipped, m.syncResult.failed)))
		}
		lines = append(lines, "")
		maxDetail := m.height - 6
		if maxDetail < 5 {
			maxDetail = 5
		}
		shown := m.syncResult.lines
		if len(shown) > maxDetail {
			shown = shown[:maxDetail]
		}
		for _, l := range shown {
			if l.ok {
				lines = append(lines, "    "+eksOkSt.Render("✔")+" "+eksDimSt.Render(l.msg))
			} else {
				lines = append(lines, "    "+eksErrSt.Render("✗")+" "+eksDimSt.Render(l.msg))
			}
		}
		if m.syncResult != nil && len(m.syncResult.lines) > maxDetail {
			lines = append(lines, "  "+eksDimSt.Render(fmt.Sprintf("... and %d more", len(m.syncResult.lines)-maxDetail)))
		}
		lines = append(lines, "")
		lines = append(lines, "  "+eksDimSt.Render("enter / esc  back to menu"))

	case eksStepViewClusters:
		lines = append(lines, "  "+eksLabelSt.Render("EKS clusters in kubeconfig")+"  "+
			eksDimSt.Render(fmt.Sprintf("(%d)", len(m.clusters)))+"  "+
			eksDimSt.Render("enter / esc to go back"))
		lines = append(lines, "")
		if len(m.clusters) == 0 {
			lines = append(lines, "  "+eksDimSt.Render("No EKS clusters found in kubeconfig."))
		} else {
			maxShow := m.height - 6
			if maxShow < 5 {
				maxShow = 5
			}
			shown := m.clusters
			if len(shown) > maxShow {
				shown = shown[:maxShow]
			}
			for _, c := range shown {
				// Extraer solo el nombre del cluster del ARN para display
				name := c.context
				if idx := strings.LastIndex(name, "/"); idx >= 0 {
					name = name[idx+1:]
				}
				region := ""
				parts := strings.Split(c.context, ":")
				if len(parts) >= 4 {
					region = parts[3]
				}
				regionStr := ""
				if region != "" {
					regionStr = "  " + eksDimSt.Render(region)
				}
				lines = append(lines, "    "+eksOkSt.Render("●")+"  "+eksNormalSt.Render(name)+regionStr)
				lines = append(lines, "       "+eksDimSt.Render(c.context))
				lines = append(lines, "")
			}
			if len(m.clusters) > 20 {
				lines = append(lines, "  "+eksDimSt.Render(fmt.Sprintf("... and %d more", len(m.clusters)-20)))
			}
		}

	case eksStepStale:
		if len(m.staleClusters) == 0 {
			lines = append(lines, "  "+eksOkSt.Render("✔ No stale clusters found"))
			lines = append(lines, "")
			lines = append(lines, "  "+eksDimSt.Render("All kubeconfig EKS contexts are still active in AWS."))
		} else {
			lines = append(lines, "  "+eksWarnSt.Render(fmt.Sprintf("⚠  Found %d stale cluster(s)", len(m.staleClusters))))
			lines = append(lines, "")
			lines = append(lines, "  "+eksDimSt.Render("These clusters no longer exist in AWS:"))
			lines = append(lines, "")
			for _, c := range m.staleClusters {
				name := c
				if idx := strings.LastIndex(name, "/"); idx >= 0 {
					name = name[idx+1:]
				}
				lines = append(lines, "    "+eksWarnSt.Render("·")+"  "+eksNormalSt.Render(name))
				lines = append(lines, "       "+eksDimSt.Render(c))
				lines = append(lines, "")
			}
			lines = append(lines, "  "+eksOkSt.Render("→ press enter")+"  "+eksDimSt.Render("to review and confirm removal"))
		}
		lines = append(lines, "  "+eksDimSt.Render("  press esc to go back"))

	case eksStepConfirmStale:
		lines = append(lines, "  "+eksWarnSt.Render(fmt.Sprintf("⚠  Remove %d stale cluster(s) from kubeconfig?", len(m.staleClusters))))
		lines = append(lines, "")
		lines = append(lines, "  "+eksDimSt.Render("This will remove these contexts from ~/.kube/config:"))
		lines = append(lines, "")
		for _, c := range m.staleClusters {
			name := c
			if idx := strings.LastIndex(name, "/"); idx >= 0 {
				name = name[idx+1:]
			}
			lines = append(lines, "    "+eksWarnSt.Render("·")+"  "+eksNormalSt.Render(name))
		}
		lines = append(lines, "")
		lines = append(lines, "  "+eksWarnSt.Render("→ press y")+"  "+eksDimSt.Render("confirm removal"))
		lines = append(lines, "  "+eksDimSt.Render("  press esc to cancel"))

	case eksStepConfirmClearAll:
		lines = append(lines, "  "+eksErrSt.Render(fmt.Sprintf("⚠  Remove ALL %d EKS cluster(s) from kubeconfig?", len(m.allEKSContexts))))
		lines = append(lines, "")
		lines = append(lines, "  "+eksDimSt.Render("This will delete ALL EKS contexts from ~/.kube/config."))
		lines = append(lines, "  "+eksDimSt.Render("You can re-add them with 'Sync all profiles'."))
		lines = append(lines, "")
		// Mostrar hasta 10 nombres
		shown := m.allEKSContexts
		if len(shown) > 10 {
			shown = shown[:10]
		}
		for _, c := range shown {
			name := c
			if idx := strings.LastIndex(name, "/"); idx >= 0 {
				name = name[idx+1:]
			}
			lines = append(lines, "    "+eksErrSt.Render("·")+"  "+eksNormalSt.Render(name))
		}
		if len(m.allEKSContexts) > 10 {
			lines = append(lines, "    "+eksDimSt.Render(fmt.Sprintf("... and %d more", len(m.allEKSContexts)-10)))
		}
		lines = append(lines, "")
		lines = append(lines, "  "+eksErrSt.Render("→ press y")+"  "+eksDimSt.Render("confirm — remove ALL"))
		lines = append(lines, "  "+eksDimSt.Render("  press esc to cancel"))
	}

	// Padding
	availH := m.height - 4
	if availH < 3 {
		availH = 3
	}
	for len(lines) < availH {
		lines = append(lines, "")
	}

	var b strings.Builder
	b.WriteString(header + "\n")
	b.WriteString(topBar + "\n")
	for _, l := range lines {
		b.WriteString(l + "\n")
	}
	b.WriteString(bottomBar)
	return b.String()
}

// ── Lógica de sync para TUI ────────────────────────────

// runEKSSyncTUIStreaming ejecuta el sync enviando líneas de progreso en vivo al canal.
// Todos los listEKSClusters y updateKubeconfig corren en paralelo.
func runEKSSyncTUIStreaming(profiles []awsProfile, progressCh chan<- eksSyncLine) eksSyncResultData {
	var res eksSyncResultData

	send := func(line eksSyncLine) {
		res.lines = append(res.lines, line)
		progressCh <- line
	}

	if err := checkAWSCLI(); err != nil {
		res.err = err
		return res
	}

	// ── Fase 1: listar clusters por perfil en paralelo (máx 32 concurrentes) ──
	const maxConcurrent = 32
	sem := make(chan struct{}, maxConcurrent)

	type profileResult struct {
		profile  awsProfile
		clusters []string
		err      error
	}
	listCh := make(chan profileResult, len(profiles))
	for _, p := range profiles {
		go func(p awsProfile) {
			sem <- struct{}{}
			clusters, err := listEKSClusters(p.Name, p.Region)
			<-sem
			listCh <- profileResult{profile: p, clusters: clusters, err: err}
		}(p)
	}

	var allDiscovered []eksCluster
	for range profiles {
		r := <-listCh
		if r.err != nil {
			send(eksSyncLine{ok: false, msg: fmt.Sprintf("profile '%s' (%s): %s", r.profile.Name, r.profile.Region, r.err.Error())})
			continue
		}
		for _, c := range r.clusters {
			allDiscovered = append(allDiscovered, eksCluster{Name: c, Profile: r.profile.Name, Region: r.profile.Region})
		}
		if len(r.clusters) == 0 {
			send(eksSyncLine{ok: true, msg: fmt.Sprintf("profile '%s' [%s] — no clusters", r.profile.Name, r.profile.Region)})
		} else {
			send(eksSyncLine{ok: true, msg: fmt.Sprintf("profile '%s' [%s] — %d cluster(s) found", r.profile.Name, r.profile.Region, len(r.clusters))})
		}
	}

	if len(allDiscovered) == 0 {
		send(eksSyncLine{ok: false, msg: "No clusters found across all profiles — check that your SSO session is active and profiles have the correct region"})
		return res
	}

	existing, _ := getExistingEKSContexts()
	newClusters, existingClusters := partitionClusters(allDiscovered, existing)

	for _, c := range existingClusters {
		send(eksSyncLine{ok: true, msg: fmt.Sprintf("skipped %s (already in kubeconfig)", c.Name)})
		res.skipped++
	}

	if len(newClusters) == 0 {
		return res
	}

	mainKubeconfig := os.Getenv("KUBECONFIG")
	if mainKubeconfig == "" {
		home, _ := os.UserHomeDir()
		mainKubeconfig = filepath.Join(home, ".kube", "config")
	}

	tmpDir, err := os.MkdirTemp("", "ksw-eks-*")
	if err != nil {
		res.err = err
		return res
	}
	defer os.RemoveAll(tmpDir)

	// ── Fase 2: update-kubeconfig para cada cluster nuevo en paralelo (máx 8) ──
	type syncOutcome struct {
		cluster eksCluster
		tmpFile string
		err     error
	}
	syncCh := make(chan syncOutcome, len(newClusters))
	for i, c := range newClusters {
		tmpFile := filepath.Join(tmpDir, fmt.Sprintf("cluster-%d.yaml", i))
		go func(c eksCluster, tmpFile string) {
			sem <- struct{}{}
			err := updateKubeconfig(c.Name, c.Profile, c.Region, tmpFile)
			<-sem
			syncCh <- syncOutcome{cluster: c, tmpFile: tmpFile, err: err}
		}(c, tmpFile)
	}

	var tmpFiles []string
	for range newClusters {
		s := <-syncCh
		if s.err != nil {
			send(eksSyncLine{ok: false, msg: fmt.Sprintf("failed %s: %s", s.cluster.Name, s.err.Error())})
			res.failed++
		} else {
			send(eksSyncLine{ok: true, cluster: s.cluster.Name, profile: s.cluster.Profile,
				msg: fmt.Sprintf("added %s  (%s)", s.cluster.Name, s.cluster.Profile)})
			tmpFiles = append(tmpFiles, s.tmpFile)
			res.added++
		}
	}

	if len(tmpFiles) > 0 {
		if err := mergeKubeconfigs(mainKubeconfig, tmpFiles); err != nil {
			res.err = err
		}
	}

	send(eksSyncLine{ok: true, msg: fmt.Sprintf(
		"Done — %d added, %d skipped, %d failed",
		res.added, res.skipped, res.failed,
	)})

	return res
}

// runEKSSyncTUI es el wrapper no-streaming (usado desde aws_sso.go para sync kubeconfig).
func runEKSSyncTUI(profiles []awsProfile) eksSyncResultData {
	progressCh := make(chan eksSyncLine, 256)
	go func() {
		for range progressCh {
		} // drena el canal sin hacer nada con las líneas
	}()
	result := runEKSSyncTUIStreaming(profiles, progressCh)
	close(progressCh)
	return result
}

// detectStaleClusters verifica cada contexto EKS del kubeconfig con describe-cluster
// directamente en paralelo — O(N contextos) sin iterar todos los profiles.
func detectStaleClusters() []string {
	existing, err := getExistingEKSContexts()
	if err != nil || len(existing) == 0 {
		return nil
	}

	type arnInfo struct {
		arn     string
		region  string
		cluster string
	}
	var toCheck []arnInfo
	for arn := range existing {
		// arn:aws:eks:<region>:<account>:cluster/<name>
		parts := strings.Split(arn, ":")
		if len(parts) < 6 {
			continue
		}
		region := parts[3]
		clusterName := strings.TrimPrefix(parts[5], "cluster/")
		if region != "" && clusterName != "" {
			toCheck = append(toCheck, arnInfo{arn: arn, region: region, cluster: clusterName})
		}
	}
	if len(toCheck) == 0 {
		return nil
	}

	// Verificar cada cluster directamente con describe-cluster en paralelo (máx 16)
	const maxConcurrent = 16
	sem := make(chan struct{}, maxConcurrent)
	type checkResult struct {
		arn    string
		exists bool
	}
	resultsCh := make(chan checkResult, len(toCheck))
	for _, a := range toCheck {
		go func(a arnInfo) {
			sem <- struct{}{}
			cmd := exec.Command("aws", "eks", "describe-cluster",
				"--name", a.cluster,
				"--region", a.region,
				"--query", "cluster.name",
				"--output", "text")
			err := cmd.Run()
			<-sem
			resultsCh <- checkResult{arn: a.arn, exists: err == nil}
		}(a)
	}

	var stale []string
	for range toCheck {
		r := <-resultsCh
		if !r.exists {
			stale = append(stale, r.arn)
		}
	}
	return stale
}

// removeStaleClusters elimina los contextos indicados del kubeconfig editando el YAML directamente,
// sin invocar kubectl por cada entrada (mucho más rápido con listas grandes).
func removeStaleClusters(stale []string) (removed int, failed int) {
	if len(stale) == 0 {
		return
	}
	toRemove := make(map[string]bool, len(stale))
	for _, s := range stale {
		toRemove[s] = true
	}

	kubePath := os.ExpandEnv("$HOME/.kube/config")
	data, err := os.ReadFile(kubePath)
	if err != nil {
		failed = len(stale)
		return
	}

	lines := strings.Split(string(data), "\n")
	var out []string
	skip := false
	depth := 0 // indentación del bloque que estamos saltando

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		if skip {
			// seguir saltando mientras la línea tenga más indentación que el bloque padre
			currentDepth := len(line) - len(strings.TrimLeft(line, " "))
			if trimmed == "" || currentDepth > depth {
				continue
			}
			skip = false
		}

		// Detectar entradas de contexto/cluster/user que coincidan
		matched := false
		for _, prefix := range []string{"- context:", "- cluster:", "- user:"} {
			if trimmed == prefix || strings.HasPrefix(trimmed, prefix+" ") {
				// mirar las siguientes líneas en busca de "name: <ctx>"
				for j := i + 1; j < len(lines) && j < i+6; j++ {
					inner := strings.TrimSpace(lines[j])
					if strings.HasPrefix(inner, "name:") {
						name := strings.TrimSpace(strings.TrimPrefix(inner, "name:"))
						if toRemove[name] {
							matched = true
							removed++
						}
						break
					}
				}
			}
			if matched {
				break
			}
		}

		if matched {
			depth = len(line) - len(strings.TrimLeft(line, " "))
			skip = true
			continue
		}

		out = append(out, line)
	}

	if err := os.WriteFile(kubePath, []byte(strings.Join(out, "\n")), 0600); err != nil {
		failed = len(stale)
		removed = 0
	}
	return
}

// ── Entry point ────────────────────────────────────────

func handleEksTUI() {
	profiles, _ := parseAWSProfiles("")
	existing, _ := getExistingEKSContexts()

	m := eksTUIModel{
		step:         eksStepMenu,
		profiles:     profiles,
		profileCount: len(profiles),
		clusterCount: len(existing),
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
