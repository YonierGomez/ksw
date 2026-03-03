package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const version = "1.3.1"

// ── Styles ─────────────────────────────────────────────
var (
	// Header
	logoStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00d4ff")).
			Background(lipgloss.Color("#1a1a2e")).
			Padding(0, 1)

	versionStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#555")).
			Padding(0, 1)

	// Current context
	currentLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#888"))

	currentValueStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#50fa7b"))

	// Search
	searchActiveStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#f1fa8c"))

	searchPlaceholderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#555")).
				Italic(true)

	// List items
	selectedItemStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00d4ff"))

	normalItemStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#999"))

	activeItemStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#50fa7b"))

	// Decorations
	aliasStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#bd93f9"))
	activeTag    = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b")).Render("●")
	pinTag       = lipgloss.NewStyle().Foreground(lipgloss.Color("#f1fa8c")).Render("★")
	pinItemStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f1fa8c"))
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
	successStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b"))
	warnStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	counterStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#666"))

	// Box
	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#333")).
			Padding(0, 1)

	helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
)

// ── Config (aliases + history + pins + groups) ────────
type config struct {
	Aliases    map[string]string   `json:"aliases"`
	History    []string            `json:"history,omitempty"`
	Previous   string              `json:"previous,omitempty"`
	Pins       []string            `json:"pins,omitempty"`
	ShortNames bool                `json:"short_names,omitempty"`
	Groups     map[string][]string `json:"groups,omitempty"`
	AI         aiConfig            `json:"ai,omitempty"`
	AIMemory   []aiMemoryEntry     `json:"ai_memory,omitempty"`
}

const maxHistory = 10

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".ksw.json")
}

func loadConfig() config {
	c := config{Aliases: make(map[string]string), Groups: make(map[string][]string)}
	data, err := os.ReadFile(configPath())
	if err != nil {
		return c
	}
	_ = json.Unmarshal(data, &c)
	if c.Aliases == nil {
		c.Aliases = make(map[string]string)
	}
	if c.Groups == nil {
		c.Groups = make(map[string][]string)
	}
	return c
}

func saveConfig(c config) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), data, 0644)
}

// recordHistory saves current context to history before switching
func recordHistory(cfg *config, current, next string) {
	if current == "" || current == next {
		return
	}
	cfg.Previous = current
	// Prepend current to history, avoid duplicates at head
	newHistory := []string{current}
	for _, h := range cfg.History {
		if h != current {
			newHistory = append(newHistory, h)
		}
	}
	if len(newHistory) > maxHistory {
		newHistory = newHistory[:maxHistory]
	}
	cfg.History = newHistory
}

// ── Fuzzy matching ─────────────────────────────────────
type scored struct {
	index int
	score int
}

// fuzzyMatch returns a score > 0 if pattern fuzzy-matches str.
// Higher score = better match. 0 = no match.
func fuzzyMatch(str, pattern string) int {
	str = strings.ToLower(str)
	pattern = strings.ToLower(pattern)

	pLen := utf8.RuneCountInString(pattern)
	if pLen == 0 {
		return 1
	}

	sRunes := []rune(str)
	pRunes := []rune(pattern)
	sLen := len(sRunes)

	// Check if all pattern chars exist in order
	pi := 0
	for si := 0; si < sLen && pi < pLen; si++ {
		if sRunes[si] == pRunes[pi] {
			pi++
		}
	}
	if pi < pLen {
		return 0 // not all chars matched
	}

	// Score: bonus for consecutive matches, word boundary matches, and early matches
	score := 0
	pi = 0
	consecutive := 0
	for si := 0; si < sLen && pi < pLen; si++ {
		if sRunes[si] == pRunes[pi] {
			pi++
			consecutive++
			score += 10 + consecutive*5 // consecutive bonus

			// Word boundary bonus (after /, -, _, or start)
			if si == 0 || sRunes[si-1] == '/' || sRunes[si-1] == '-' || sRunes[si-1] == '_' {
				score += 20
			}
			// Early match bonus
			score += max(0, 5-si)
		} else {
			consecutive = 0
		}
	}

	// Exact substring bonus
	if strings.Contains(str, pattern) {
		score += 50
	}

	return score
}

// ── Kubeconfig helpers ─────────────────────────────────
func getContexts() ([]string, error) {
	cmd := exec.Command("kubectl", "config", "get-contexts", "-o", "name")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get contexts: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var contexts []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			contexts = append(contexts, l)
		}
	}
	return contexts, nil
}

func getCurrentContext() string {
	cmd := exec.Command("kubectl", "config", "current-context")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func switchContext(name string) error {
	cmd := exec.Command("kubectl", "config", "use-context", name)
	return cmd.Run()
}

// ── Model ──────────────────────────────────────────────
type model struct {
	contexts       []string
	filtered       []int
	cursor         int
	scrollOffset   int
	current        string
	chosen         string
	search         string
	cfg            config
	terminalHeight int
	terminalWidth  int
	quitting       bool
	shortNames      bool
	activeGroup     string // "" = all contexts
	showPinnedOnly  bool   // Ctrl+F toggle
}

// shortName extracts the last segment after '/' from a context name
func shortName(ctx string) string {
	if idx := strings.LastIndex(ctx, "/"); idx >= 0 {
		return ctx[idx+1:]
	}
	return ctx
}

func initialModel(contexts []string, current string, cfg config, activeGroup string, pinnedOnly bool) model {
	m := model{
		contexts:       contexts,
		current:        current,
		cfg:            cfg,
		terminalHeight: 24,
		terminalWidth:  80,
		shortNames:     cfg.ShortNames,
		activeGroup:    activeGroup,
		showPinnedOnly: pinnedOnly,
	}
	m.resetFilter()
	for i, idx := range m.filtered {
		if contexts[idx] == current {
			m.cursor = i
			break
		}
	}
	m.ensureVisible()
	return m
}

// isPinned returns true if ctx is in the pins list
func (m *model) isPinned(ctx string) bool {
	for _, p := range m.cfg.Pins {
		if p == ctx {
			return true
		}
	}
	return false
}

// sortedByPins returns indices with pinned contexts first (preserving pin order), then the rest
func (m *model) sortedByPins(indices []int) []int {
	pinSet := make(map[string]int, len(m.cfg.Pins))
	for i, p := range m.cfg.Pins {
		pinSet[p] = i
	}
	pinned := make([]int, 0, len(m.cfg.Pins))
	rest := make([]int, 0, len(indices))
	// collect pinned in pin order
	for _, p := range m.cfg.Pins {
		for _, idx := range indices {
			if m.contexts[idx] == p {
				pinned = append(pinned, idx)
				break
			}
		}
	}
	// collect rest
	for _, idx := range indices {
		if _, ok := pinSet[m.contexts[idx]]; !ok {
			rest = append(rest, idx)
		}
	}
	return append(pinned, rest...)
}

// groupSet returns the set of contexts in the active group (nil = all)
func (m *model) groupSet() map[string]bool {
	if m.activeGroup == "" {
		return nil
	}
	members := m.cfg.Groups[m.activeGroup]
	set := make(map[string]bool, len(members))
	for _, c := range members {
		set[c] = true
	}
	return set
}

func (m *model) resetFilter() {
	gs := m.groupSet()
	var indices []int
	for i, ctx := range m.contexts {
		if gs != nil && !gs[ctx] {
			continue
		}
		if m.showPinnedOnly && !m.isPinned(ctx) {
			continue
		}
		indices = append(indices, i)
	}
	m.filtered = m.sortedByPins(indices)
	m.scrollOffset = 0
}

func (m *model) applyFilter() {
	if m.search == "" {
		m.resetFilter()
		return
	}

	query := m.search
	gs := m.groupSet()

	// Build searchable strings: context name + any aliases pointing to it
	reverseAlias := make(map[string][]string)
	for alias, ctx := range m.cfg.Aliases {
		reverseAlias[ctx] = append(reverseAlias[ctx], alias)
	}

	var results []scored
	for i, ctx := range m.contexts {
		if gs != nil && !gs[ctx] {
			continue
		}
		if m.showPinnedOnly && !m.isPinned(ctx) {
			continue
		}
		// Match against context name
		searchable := ctx
		if aliases, ok := reverseAlias[ctx]; ok {
			searchable += " " + strings.Join(aliases, " ")
		}
		score := fuzzyMatch(searchable, query)
		if score > 0 {
			results = append(results, scored{index: i, score: score})
		}
	}

	// Sort by score descending
	sort.Slice(results, func(a, b int) bool {
		return results[a].score > results[b].score
	})

	indices := make([]int, 0, len(results))
	for _, r := range results {
		indices = append(indices, r.index)
	}
	m.filtered = m.sortedByPins(indices)
	if m.cursor >= len(m.filtered) {
		m.cursor = max(0, len(m.filtered)-1)
	}
}

func (m *model) maxVisible() int {
	headerLines := 8
	v := m.terminalHeight - headerLines - 2
	if v < 3 {
		v = 3
	}
	return v
}

func (m *model) ensureVisible() {
	mv := m.maxVisible()
	if m.cursor < m.scrollOffset {
		m.scrollOffset = m.cursor
	} else if m.cursor >= m.scrollOffset+mv {
		m.scrollOffset = m.cursor - mv + 1
	}
}

func (m *model) aliasFor(ctx string) string {
	for alias, target := range m.cfg.Aliases {
		if target == ctx {
			return alias
		}
	}
	return ""
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.terminalHeight = msg.Height
		m.terminalWidth = msg.Width

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			m.quitting = true
			return m, tea.Quit
		case tea.KeyEscape:
			if m.search != "" {
				m.search = ""
				m.resetFilter()
				m.cursor = 0
			} else {
				m.quitting = true
				return m, tea.Quit
			}
		case tea.KeyUp:
			if m.cursor > 0 {
				m.cursor--
				m.ensureVisible()
			}
		case tea.KeyDown:
			if m.cursor < len(m.filtered)-1 {
				m.cursor++
				m.ensureVisible()
			}
		case tea.KeyHome:
			m.cursor = 0
			m.ensureVisible()
		case tea.KeyEnd:
			m.cursor = max(0, len(m.filtered)-1)
			m.ensureVisible()
		case tea.KeyPgUp:
			m.cursor = max(0, m.cursor-10)
			m.ensureVisible()
		case tea.KeyPgDown:
			m.cursor = min(len(m.filtered)-1, m.cursor+10)
			m.ensureVisible()
		case tea.KeyCtrlP:
			// Toggle pin/unpin on the current item
			if len(m.filtered) > 0 {
				ctx := m.contexts[m.filtered[m.cursor]]
				if m.isPinned(ctx) {
					newPins := make([]string, 0, len(m.cfg.Pins))
					for _, p := range m.cfg.Pins {
						if p != ctx {
							newPins = append(newPins, p)
						}
					}
					m.cfg.Pins = newPins
				} else {
					m.cfg.Pins = append(m.cfg.Pins, ctx)
				}
				_ = saveConfig(m.cfg)
				savedCtx := ctx
				m.resetFilter()
				for i, idx := range m.filtered {
					if m.contexts[idx] == savedCtx {
						m.cursor = i
						break
					}
				}
				m.ensureVisible()
			}
		case tea.KeyCtrlT:
			// Jump to first pinned context
			for i, idx := range m.filtered {
				if m.isPinned(m.contexts[idx]) {
					m.cursor = i
					m.ensureVisible()
					break
				}
			}
		case tea.KeyCtrlH:
			// Toggle short name view and persist
			m.shortNames = !m.shortNames
			m.cfg.ShortNames = m.shortNames
			_ = saveConfig(m.cfg)
		case tea.KeyCtrlF:
			// Toggle pinned-only filter
			m.showPinnedOnly = !m.showPinnedOnly
			m.search = ""
			m.resetFilter()
			m.cursor = 0
			m.scrollOffset = 0
		case tea.KeyEnter:
			if len(m.filtered) > 0 {
				m.chosen = m.contexts[m.filtered[m.cursor]]
				return m, tea.Quit
			}
		case tea.KeyBackspace:
			if len(m.search) > 0 {
				m.search = m.search[:len(m.search)-1]
				m.applyFilter()
			}
		case tea.KeyRunes:
			m.search += string(msg.Runes)
			m.applyFilter()
			m.cursor = 0
			m.scrollOffset = 0
			// Note: KeyCtrlP and KeyCtrlT are handled above, not here
		}
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting || m.chosen != "" {
		return ""
	}

	var b strings.Builder

	// ── Current context ──
	currentAlias := m.aliasFor(m.current)
	currentName := m.current
	if m.shortNames {
		currentName = shortName(m.current)
	}
	if currentAlias != "" {
		currentName += " " + aliasStyle.Render("@"+currentAlias)
	}
	var currentDisplay string
	if m.shortNames {
		currentDisplay = dimStyle.Render("[short] ") + currentValueStyle.Render(currentName)
	} else {
		currentDisplay = currentValueStyle.Render(currentName)
	}
	filterLabel := ""
	if m.activeGroup != "" {
		filterLabel = "  " + pinItemStyle.Render("["+m.activeGroup+"]")
	} else if m.showPinnedOnly {
		filterLabel = "  " + pinItemStyle.Render("[★ pinned]")
	}
	b.WriteString("  " + currentLabelStyle.Render("  current ") + currentDisplay + filterLabel + "\n")
	b.WriteString("\n")

	// ── Search bar ──
	if m.search != "" {
		b.WriteString("  " + searchActiveStyle.Render("  ❯ "+m.search+"█") + "\n")
	} else {
		b.WriteString("  " + searchPlaceholderStyle.Render("  ❯ type to search...") + "\n")
	}

	// ── Separator ──
	b.WriteString("  " + dimStyle.Render("  ─────────────────────────────────────────") + "\n")

	if len(m.filtered) == 0 {
		b.WriteString("\n  " + dimStyle.Render("  No matching contexts") + "\n")
		return b.String()
	}

	maxVisible := m.maxVisible()

	start := m.scrollOffset
	end := start + maxVisible
	if end > len(m.filtered) {
		end = len(m.filtered)
	}

	// ── Scroll indicator top ──
	if start > 0 {
		b.WriteString("  " + dimStyle.Render(fmt.Sprintf("    ▲ %d more", start)) + "\n")
	}

	// ── List ──
	for i := start; i < end; i++ {
		ctx := m.contexts[m.filtered[i]]
		isActive := ctx == m.current
		alias := m.aliasFor(ctx)

		pointer := "   "
		var name string

		isPinned := m.isPinned(ctx)

		displayCtx := ctx
		if m.shortNames {
			displayCtx = shortName(ctx)
		}

		if i == m.cursor {
			pointer = " ❯ "
			name = selectedItemStyle.Render(displayCtx)
		} else if isActive {
			name = activeItemStyle.Render(displayCtx)
		} else if isPinned {
			name = pinItemStyle.Render(displayCtx)
		} else {
			name = normalItemStyle.Render(displayCtx)
		}

		extras := ""
		if alias != "" {
			extras += " " + aliasStyle.Render("@"+alias)
		}
		if isPinned {
			extras += " " + pinTag
		}
		if isActive {
			extras += " " + activeTag
		}

		b.WriteString("  " + pointer + name + extras + "\n")
	}

	// ── Scroll indicator bottom ──
	if end < len(m.filtered) {
		b.WriteString("  " + dimStyle.Render(fmt.Sprintf("    ▼ %d more", len(m.filtered)-end)) + "\n")
	}

	// ── Footer ──
	b.WriteString("\n")
	counter := counterStyle.Render(fmt.Sprintf("  %d/%d", len(m.filtered), len(m.contexts)))
	var help string
	if m.terminalWidth >= 120 {
		help = "  ↑↓ navigate · enter select · ctrl+p pin/unpin · ctrl+t jump-pin · ctrl+f pinned · ctrl+h short · esc · ctrl+c quit"
	} else if m.terminalWidth >= 80 {
		help = "  ↑↓ · enter · ^p pin · ^t pins · ^f pinned · ^h short · esc · ^c quit"
	} else {
		help = "  ↑↓ enter · ^p pin · ^f pinned · ^h short · esc ^c"
	}
	b.WriteString("  " + counter + helpStyle.Render(help) + "\n")

	return b.String()
}

// ── Main ───────────────────────────────────────────────
func main() {
	cfg := loadConfig()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "-v", "--version":
			fmt.Printf("ksw v%s\n", version)
			return

		case "-h", "--help":
			fmt.Printf(`ksw v%s - Interactive Kubernetes context switcher

Usage:
  ksw                        Launch interactive selector (fuzzy search)
  ksw <name>                 Switch directly to context <name> (short name ok)
  ksw -                      Switch to previous context
  ksw @<alias>               Switch using an alias
  ksw history                Show recent context history
  ksw history <n>            Switch to history entry by number
  ksw group add <name> [ctx] Create a group (use quotes for glob: "eks-sufi*")
  ksw group rm <name>        Remove a group
  ksw group ls               List all groups
  ksw group use <name>       Open TUI filtered to a group
  ksw group add-ctx <g> <ctx> Add a context to an existing group
  ksw group rmi <g> <ctx>  Remove a context from a group
  ksw pin <name>             Pin a context to the top of the list
  ksw pin rm <name>          Unpin a context
  ksw pin ls                 List pinned contexts
  ksw pin use                Open TUI filtered to pinned contexts only
  ksw rename <old> <new>     Rename a context in kubeconfig
  ksw alias <name> <context> Create alias for a context
  ksw alias rm <name>        Remove an alias
  ksw alias ls               List all aliases
  ksw completion install     Auto-install completion in ~/.zshrc or ~/.bashrc
  ksw completion zsh         Print zsh setup line
  ksw completion bash        Print bash setup line
  ksw ai "<query>"           Switch context using natural language (AI)
  ksw ai config              Configure AI provider (openai, claude, gemini)
  ksw -l                     List contexts (non-interactive)
  ksw -h                     Show this help
  ksw -v                     Show version

Navigation:
  Type                Filter contexts with fuzzy search
  ↑ / ↓               Move up / down
  Home / End          Go to top / bottom
  PgUp / PgDn         Jump 10 items
  Backspace           Delete last character from filter
  Enter               Switch to highlighted context
  Esc                 Clear filter / Quit
  Ctrl+C              Quit

Config stored in ~/.ksw.json
`, version)
			return

		case "-l", "--list":
			contexts, err := getContexts()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			current := getCurrentContext()
			reverseAlias := make(map[string]string)
			for alias, ctx := range cfg.Aliases {
				reverseAlias[ctx] = alias
			}
			for _, ctx := range contexts {
				alias := ""
				if a, ok := reverseAlias[ctx]; ok {
					alias = aliasStyle.Render(" @" + a)
				}
				if ctx == current {
					fmt.Printf("%s%s %s\n", currentValueStyle.Render("▸ "+ctx), alias, activeTag)
				} else {
					fmt.Printf("  %s%s\n", ctx, alias)
				}
			}
			return

		case "-":
			// Switch to previous context
			if cfg.Previous == "" {
				fmt.Fprintf(os.Stderr, "%s No previous context recorded.\n", warnStyle.Render("✗"))
				os.Exit(1)
			}
			current := getCurrentContext()
			prev := cfg.Previous
			recordHistory(&cfg, current, prev)
			if err := switchContext(prev); err != nil {
				fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), prev)
				os.Exit(1)
			}
			if err := saveConfig(cfg); err != nil {
				fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("%s Switched to %s\n", successStyle.Render("✔"), prev)
			return

		case "history":
			if len(cfg.History) == 0 {
				fmt.Println(dimStyle.Render("No history yet."))
				return
			}
			current := getCurrentContext()
			reverseAlias := make(map[string]string)
			for alias, ctx := range cfg.Aliases {
				reverseAlias[ctx] = alias
			}

			// If a number is provided, switch to that history entry
			if len(os.Args) >= 3 {
				n := 0
				for _, c := range os.Args[2] {
					if c < '0' || c > '9' {
						fmt.Fprintf(os.Stderr, "%s Invalid number '%s'. Usage: ksw history <number>\n", warnStyle.Render("✗"), os.Args[2])
						os.Exit(1)
					}
					n = n*10 + int(c-'0')
				}
				if n < 1 || n > len(cfg.History) {
					fmt.Fprintf(os.Stderr, "%s Number must be between 1 and %d\n", warnStyle.Render("✗"), len(cfg.History))
					os.Exit(1)
				}
				target := cfg.History[n-1]
				recordHistory(&cfg, current, target)
				if err := switchContext(target); err != nil {
					// Try suffix/substring match
					contexts, cerr := getContexts()
					if cerr != nil {
						fmt.Fprintln(os.Stderr, cerr)
						os.Exit(1)
					}
					var matches []string
					for _, ctx := range contexts {
						if strings.HasSuffix(ctx, "/"+target) || strings.Contains(ctx, target) {
							matches = append(matches, ctx)
						}
					}
					if len(matches) == 1 {
						target = matches[0]
						if err := switchContext(target); err != nil {
							fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), target)
							os.Exit(1)
						}
					} else {
						fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), target)
						os.Exit(1)
					}
				}
				_ = saveConfig(cfg)
				alias := ""
				if a, ok := reverseAlias[target]; ok {
					alias = " " + aliasStyle.Render("@"+a)
				}
				fmt.Printf("%s Switched to %s%s\n", successStyle.Render("✔"), target, alias)
				return
			}

			// Otherwise just list history
			fmt.Println(dimStyle.Render("  Recent contexts:"))
			for i, ctx := range cfg.History {
				name := normalItemStyle.Render(ctx)
				if ctx == current {
					name = activeItemStyle.Render(ctx)
				}
				alias := ""
				if a, ok := reverseAlias[ctx]; ok {
					alias = " " + aliasStyle.Render("@"+a)
				}
				active := ""
				if ctx == current {
					active = " " + activeTag
				}
				fmt.Printf("  %d  %s%s%s\n", i+1, name, alias, active)
			}
			return

		case "rename":
			handleRename(cfg)
			return

		case "completion":
			handleCompletion()
			return

		case "pin":
			handlePin(cfg)
			return

		case "group":
			handleGroup(cfg)
			return


		case "alias":
			handleAlias(cfg)
			return

		case "ai":
			handleAI(cfg)
			return

		default:
			arg := os.Args[1]

			// Handle @alias
			if strings.HasPrefix(arg, "@") {
				aliasName := arg[1:]
				target, ok := cfg.Aliases[aliasName]
				if !ok {
					fmt.Fprintf(os.Stderr, "%s Alias '%s' not found. Use 'ksw alias ls' to list.\n", warnStyle.Render("✗"), aliasName)
					os.Exit(1)
				}
				// Try exact match first, then suffix/substring match
				if err := switchContext(target); err != nil {
					contexts, cerr := getContexts()
					if cerr != nil {
						fmt.Fprintln(os.Stderr, cerr)
						os.Exit(1)
					}
					var matches []string
					for _, ctx := range contexts {
						if strings.HasSuffix(ctx, "/"+target) || strings.HasSuffix(ctx, target) || strings.Contains(ctx, target) {
							matches = append(matches, ctx)
						}
					}
					if len(matches) == 1 {
						target = matches[0]
						if err := switchContext(target); err != nil {
							fmt.Fprintf(os.Stderr, "%s Context '%s' (alias @%s) not found in kubeconfig.\n", warnStyle.Render("✗"), target, aliasName)
							os.Exit(1)
						}
					} else if len(matches) > 1 {
						fmt.Fprintf(os.Stderr, "%s Ambiguous alias @%s, matches:\n", warnStyle.Render("✗"), aliasName)
						for _, m := range matches {
							fmt.Fprintf(os.Stderr, "  %s\n", m)
						}
						os.Exit(1)
					} else {
						fmt.Fprintf(os.Stderr, "%s Context '%s' (alias @%s) not found in kubeconfig.\n", warnStyle.Render("✗"), target, aliasName)
						os.Exit(1)
					}
				}
				current := getCurrentContext()
				recordHistory(&cfg, current, target)
				_ = saveConfig(cfg)
				fmt.Printf("%s Switched to %s %s\n", successStyle.Render("✔"), target, aliasStyle.Render("@"+aliasName))
				return
			}

			if arg[0] != '-' {
				// Try exact match first, then suffix/substring match
				current := getCurrentContext()
				target := arg
				if err := switchContext(target); err != nil {
					// Exact match failed, try to find by suffix or substring
					contexts, cerr := getContexts()
					if cerr != nil {
						fmt.Fprintln(os.Stderr, cerr)
						os.Exit(1)
					}
					var matches []string
					for _, ctx := range contexts {
						if strings.HasSuffix(ctx, "/"+arg) || strings.HasSuffix(ctx, arg) || strings.Contains(ctx, arg) {
							matches = append(matches, ctx)
						}
					}
					if len(matches) == 1 {
						target = matches[0]
						if err := switchContext(target); err != nil {
							fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), target)
							os.Exit(1)
						}
					} else if len(matches) > 1 {
						fmt.Fprintf(os.Stderr, "%s Ambiguous context '%s', matches:\n", warnStyle.Render("✗"), arg)
						for _, m := range matches {
							fmt.Fprintf(os.Stderr, "  %s\n", m)
						}
						os.Exit(1)
					} else {
						fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), arg)
						os.Exit(1)
					}
				}
				recordHistory(&cfg, current, target)
				_ = saveConfig(cfg)
				fmt.Printf("%s Switched to %s\n", successStyle.Render("✔"), target)
				return
			}
			fmt.Fprintf(os.Stderr, "Unknown flag: %s. Use -h for help.\n", arg)
			os.Exit(1)
		}
	}

	// Interactive mode
	contexts, err := getContexts()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(contexts) == 0 {
		fmt.Fprintln(os.Stderr, "No contexts found in kubeconfig.")
		os.Exit(1)
	}

	current := getCurrentContext()
	m := initialModel(contexts, current, cfg, "", false)

	p := tea.NewProgram(m, tea.WithAltScreen())
	result, err := p.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	final := result.(model)
	if final.chosen != "" && final.chosen != current {
		recordHistory(&final.cfg, current, final.chosen)
		if err := switchContext(final.chosen); err != nil {
			fmt.Fprintf(os.Stderr, "Error switching to %s: %v\n", final.chosen, err)
			os.Exit(1)
		}
		_ = saveConfig(final.cfg)
		alias := final.aliasFor(final.chosen)
		extra := ""
		if alias != "" {
			extra = " " + aliasStyle.Render("@"+alias)
		}
		fmt.Printf("%s Switched to %s%s\n", successStyle.Render("✔"), final.chosen, extra)
	} else if final.chosen == current {
		fmt.Printf("%s Already on %s\n", dimStyle.Render("·"), current)
	}
}

// ── handleRename ───────────────────────────────────────
func handleRename(cfg config) {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "Usage: ksw rename <old-name> <new-name>")
		os.Exit(1)
	}
	oldName := os.Args[2]
	newName := os.Args[3]

	// Get all contexts to find the full name if short name given
	contexts, err := getContexts()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Resolve old name (exact or suffix/substring)
	resolvedOld := oldName
	if err := switchContext(oldName); err != nil {
		// Not exact, try substring
		var matches []string
		for _, ctx := range contexts {
			if strings.HasSuffix(ctx, "/"+oldName) || strings.Contains(ctx, oldName) {
				matches = append(matches, ctx)
			}
		}
		if len(matches) == 1 {
			resolvedOld = matches[0]
		} else if len(matches) > 1 {
			fmt.Fprintf(os.Stderr, "%s Ambiguous name '%s', matches:\n", warnStyle.Render("✗"), oldName)
			for _, m := range matches {
				fmt.Fprintf(os.Stderr, "  %s\n", m)
			}
			os.Exit(1)
		} else {
			fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), oldName)
			os.Exit(1)
		}
	}
	// Switch back to current after the test switch above
	if cur := getCurrentContext(); cur != resolvedOld {
		_ = switchContext(cur)
	}

	cmd := exec.Command("kubectl", "config", "rename-context", resolvedOld, newName)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to rename: %s\n", warnStyle.Render("✗"), strings.TrimSpace(string(out)))
		os.Exit(1)
	}

	// Update aliases that pointed to old name
	updated := 0
	for alias, target := range cfg.Aliases {
		if target == resolvedOld {
			cfg.Aliases[alias] = newName
			updated++
		}
	}
	// Update history
	for i, h := range cfg.History {
		if h == resolvedOld {
			cfg.History[i] = newName
		}
	}
	if cfg.Previous == resolvedOld {
		cfg.Previous = newName
	}
	if err := saveConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s Renamed %s → %s\n", successStyle.Render("✔"),
		dimStyle.Render(resolvedOld), currentValueStyle.Render(newName))
	if updated > 0 {
		fmt.Printf("  %s Updated %d alias(es)\n", dimStyle.Render("·"), updated)
	}
}

// ── handleCompletion ───────────────────────────────────
func handleCompletion() {
	shell := "zsh"
	if len(os.Args) >= 3 {
		shell = os.Args[2]
	}

	// If --script flag passed, print the actual completion script (used by source <(...))
	if len(os.Args) >= 4 && os.Args[3] == "--script" {
		printCompletionScript(shell)
		return
	}

	// "install" subcommand: auto-install into shell rc file
	if shell == "install" {
		installCompletion()
		return
	}

	// Otherwise just print the line to add to shell config
	switch shell {
	case "zsh":
		fmt.Println("# Add this line to your ~/.zshrc:")
		fmt.Println("source <(ksw completion zsh --script)")
	case "bash":
		fmt.Println("# Add this line to your ~/.bashrc:")
		fmt.Println("source <(ksw completion bash --script)")
	default:
		fmt.Fprintf(os.Stderr, "Unknown shell '%s'. Supported: zsh, bash, install\n", shell)
		os.Exit(1)
	}
}

func installCompletion() {
	// Detect shell from $SHELL env var
	shellBin := os.Getenv("SHELL")
	var rcFile, shellName string
	switch {
	case strings.HasSuffix(shellBin, "zsh"):
		shellName = "zsh"
		home, _ := os.UserHomeDir()
		rcFile = filepath.Join(home, ".zshrc")
	case strings.HasSuffix(shellBin, "bash"):
		shellName = "bash"
		home, _ := os.UserHomeDir()
		rcFile = filepath.Join(home, ".bashrc")
	default:
		fmt.Fprintf(os.Stderr, "%s Could not detect shell (SHELL=%s). Run manually:\n", warnStyle.Render("✗"), shellBin)
		fmt.Fprintf(os.Stderr, "  ksw completion zsh   # for zsh\n")
		fmt.Fprintf(os.Stderr, "  ksw completion bash  # for bash\n")
		os.Exit(1)
	}

	line := fmt.Sprintf("source <(ksw completion %s --script)", shellName)
	marker := "# ksw completion"

	// Read existing rc file
	data, err := os.ReadFile(rcFile)
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "%s Could not read %s: %v\n", warnStyle.Render("✗"), rcFile, err)
		os.Exit(1)
	}

	// Check if already installed (idempotent)
	if strings.Contains(string(data), line) {
		fmt.Printf("%s Completion already installed in %s\n", dimStyle.Render("·"), rcFile)
		fmt.Printf("  Run: %s\n", searchActiveStyle.Render("source "+rcFile))
		return
	}

	// Append to rc file
	f, err := os.OpenFile(rcFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Could not write to %s: %v\n", warnStyle.Render("✗"), rcFile, err)
		os.Exit(1)
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "\n%s\n%s\n", marker, line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Could not write completion: %v\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	fmt.Printf("%s Installed %s completion in %s\n", successStyle.Render("✔"), shellName, currentValueStyle.Render(rcFile))
	fmt.Printf("  Run: %s\n", searchActiveStyle.Render("source "+rcFile))
}

func printCompletionScript(shell string) {
	switch shell {
	case "zsh":
		fmt.Print(`_ksw_contexts() {
  local contexts
  contexts=($(kubectl config get-contexts -o name 2>/dev/null))
  _describe 'contexts' contexts
}

_ksw_aliases() {
  local aliases
  aliases=($(ksw alias ls 2>/dev/null | awk '{print $1}' | tr -d '@'))
  _describe 'aliases' aliases
}

_ksw_groups() {
  local groups
  groups=($(ksw group ls 2>/dev/null | awk '{print $1}'))
  _describe 'groups' groups
}

_ksw() {
  local state
  _arguments \
    '1: :->cmd' \
    '*: :->args' && return

  case $state in
    cmd)
      local cmds
      cmds=(
        'history:Show recent context history'
        'group:Manage context groups'
        'pin:Pin contexts to the top of the list'
        'alias:Manage aliases'
        'rename:Rename a context'
        'completion:Print shell completion setup'
        '-:Switch to previous context'
        '-l:List contexts'
        '-v:Show version'
        '-h:Show help'
      )
      _describe 'commands' cmds
      _ksw_contexts
      ;;
    args)
      case $words[2] in
        alias)
          if [[ ${#words[@]} -eq 3 ]]; then
            local sub=(ls rm)
            _describe 'subcommands' sub
            _ksw_aliases
          elif [[ ${#words[@]} -eq 4 && $words[3] == rm ]]; then
            _ksw_aliases
          fi
          ;;
        group)
          if [[ ${#words[@]} -eq 3 ]]; then
            local sub=(add rm ls use add-ctx rmi)
            _describe 'subcommands' sub
          elif [[ ${#words[@]} -ge 4 ]]; then
            case $words[3] in
              use|rm|add-ctx|rmi) _ksw_groups ;;
            esac
          fi
          ;;
        pin)
          if [[ ${#words[@]} -eq 3 ]]; then
            local sub=(ls rm use)
            _describe 'subcommands' sub
            _ksw_contexts
          fi
          ;;
        rename)
          _ksw_contexts ;;
      esac
      ;;
  esac
}

compdef _ksw ksw
`)
	case "bash":
		fmt.Print(`_ksw_complete() {
  local cur prev pprev
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  pprev="${COMP_WORDS[COMP_CWORD-2]}"

  local contexts
  contexts=$(kubectl config get-contexts -o name 2>/dev/null | tr '\n' ' ')

  local aliases
  aliases=$(ksw alias ls 2>/dev/null | awk '{print $1}' | tr -d '@' | tr '\n' ' ')

  local groups
  groups=$(ksw group ls 2>/dev/null | awk '{print $1}' | tr '\n' ' ')

  if [[ $COMP_CWORD -eq 1 ]]; then
    local cmds="history group pin alias rename completion - -l -v -h"
    COMPREPLY=( $(compgen -W "$cmds $contexts" -- "$cur") )
    return
  fi

  case "$prev" in
    group)  COMPREPLY=( $(compgen -W "add rm ls use add-ctx rmi" -- "$cur") ) ;;
    pin)    COMPREPLY=( $(compgen -W "ls rm use $contexts" -- "$cur") ) ;;
    alias)  COMPREPLY=( $(compgen -W "ls rm $aliases" -- "$cur") ) ;;
    use)    [[ "$pprev" == "group" ]] && COMPREPLY=( $(compgen -W "$groups" -- "$cur") ) ;;
    rm)
      case "$pprev" in
        alias) COMPREPLY=( $(compgen -W "$aliases" -- "$cur") ) ;;
        group) COMPREPLY=( $(compgen -W "$groups" -- "$cur") ) ;;
        pin)   COMPREPLY=( $(compgen -W "$contexts" -- "$cur") ) ;;
      esac
      ;;
    rename|add-ctx|rmi) COMPREPLY=( $(compgen -W "$contexts" -- "$cur") ) ;;
    *)      COMPREPLY=( $(compgen -W "$contexts" -- "$cur") ) ;;
  esac
}

complete -F _ksw_complete ksw
`)
	}
}

// ── handlePin ──────────────────────────────────────────
func handlePin(cfg config) {
	if len(os.Args) < 3 {
		// No subcommand: list pins
		if len(cfg.Pins) == 0 {
			fmt.Println(dimStyle.Render("No pinned contexts. Use: ksw pin <name>"))
			return
		}
		for _, p := range cfg.Pins {
			fmt.Printf("  %s %s\n", pinTag, pinItemStyle.Render(p))
		}
		return
	}

	sub := os.Args[2]

	switch sub {
	case "ls", "list":
		if len(cfg.Pins) == 0 {
			fmt.Println(dimStyle.Render("No pinned contexts. Use: ksw pin <name>"))
			return
		}
		for _, p := range cfg.Pins {
			fmt.Printf("  %s %s\n", pinTag, pinItemStyle.Render(p))
		}

	case "use":
		// ksw pin use — open TUI filtered to pinned contexts
		if len(cfg.Pins) == 0 {
			fmt.Fprintf(os.Stderr, "%s No pinned contexts. Use 'ksw pin <name>' to pin first.\n", warnStyle.Render("✗"))
			os.Exit(1)
		}
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		current := getCurrentContext()
		m := initialModel(contexts, current, cfg, "", true)
		p := tea.NewProgram(m, tea.WithAltScreen())
		result, err := p.Run()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		final := result.(model)
		if final.chosen != "" && final.chosen != current {
			recordHistory(&final.cfg, current, final.chosen)
			if err := switchContext(final.chosen); err != nil {
				fmt.Fprintf(os.Stderr, "Error switching to %s: %v\n", final.chosen, err)
				os.Exit(1)
			}
			_ = saveConfig(final.cfg)
			alias := final.aliasFor(final.chosen)
			extra := ""
			if alias != "" {
				extra = " " + aliasStyle.Render("@"+alias)
			}
			fmt.Printf("%s Switched to %s%s\n", successStyle.Render("✔"), final.chosen, extra)
		} else if final.chosen == current {
			fmt.Printf("%s Already on %s\n", dimStyle.Render("·"), current)
		}

	case "rm", "remove", "unpin":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: ksw pin rm <name>")
			os.Exit(1)
		}
		name := os.Args[3]
		// Resolve short name
		resolved := name
		for _, p := range cfg.Pins {
			if strings.HasSuffix(p, "/"+name) || strings.Contains(p, name) {
				resolved = p
				break
			}
		}
		found := false
		newPins := cfg.Pins[:0]
		for _, p := range cfg.Pins {
			if p == resolved {
				found = true
			} else {
				newPins = append(newPins, p)
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "%s '%s' is not pinned.\n", warnStyle.Render("✗"), name)
			os.Exit(1)
		}
		cfg.Pins = newPins
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s Unpinned %s\n", successStyle.Render("✔"), resolved)

	default:
		// ksw pin <name> — add pin
		name := sub
		// Resolve full context name (exact or suffix/substring)
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		resolved := name
		// Check exact match first
		exactFound := false
		for _, ctx := range contexts {
			if ctx == name {
				exactFound = true
				break
			}
		}
		if !exactFound {
			var matches []string
			for _, ctx := range contexts {
				if strings.HasSuffix(ctx, "/"+name) || strings.Contains(ctx, name) {
					matches = append(matches, ctx)
				}
			}
			if len(matches) == 1 {
				resolved = matches[0]
			} else if len(matches) > 1 {
				fmt.Fprintf(os.Stderr, "%s Ambiguous '%s', matches:\n", warnStyle.Render("✗"), name)
				for _, m := range matches {
					fmt.Fprintf(os.Stderr, "  %s\n", m)
				}
				os.Exit(1)
			} else {
				fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), name)
				os.Exit(1)
			}
		}
		// Check already pinned
		for _, p := range cfg.Pins {
			if p == resolved {
				fmt.Printf("%s Already pinned: %s\n", dimStyle.Render("·"), resolved)
				return
			}
		}
		cfg.Pins = append(cfg.Pins, resolved)
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s Pinned %s %s\n", successStyle.Render("✔"), pinTag, pinItemStyle.Render(resolved))
	}
}

// ── handleGroup ────────────────────────────────────────

// globMatch returns true if str matches a simple glob pattern (* and ?)
// matching is done against the full context name and also the short name (after last /)
func globMatch(pattern, str string) bool {
	// match against full name
	if matchGlob(pattern, str) {
		return true
	}
	// match against short name (after last /)
	short := str
	if idx := strings.LastIndex(str, "/"); idx >= 0 {
		short = str[idx+1:]
	}
	return matchGlob(pattern, short)
}

// matchGlob is a simple glob matcher supporting * and ?
func matchGlob(pattern, str string) bool {
	p, s := 0, 0
	starIdx := -1
	match := 0
	for s < len(str) {
		if p < len(pattern) && (pattern[p] == '?' || pattern[p] == str[s]) {
			p++
			s++
		} else if p < len(pattern) && pattern[p] == '*' {
			starIdx = p
			match = s
			p++
		} else if starIdx != -1 {
			p = starIdx + 1
			match++
			s = match
		} else {
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// resolveContexts resolves a name/pattern to one or more context names.
// If the pattern contains * or ?, it returns all matching contexts.
// Otherwise it returns exactly one context (or error).
func resolveContexts(name string, contexts []string) ([]string, error) {
	// Glob pattern
	if strings.ContainsAny(name, "*?") {
		var matches []string
		for _, ctx := range contexts {
			if globMatch(name, ctx) {
				matches = append(matches, ctx)
			}
		}
		// If no matches and pattern doesn't start with *, try *pattern
		if len(matches) == 0 && !strings.HasPrefix(name, "*") {
			wrapped := "*" + name
			for _, ctx := range contexts {
				if globMatch(wrapped, ctx) {
					matches = append(matches, ctx)
				}
			}
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no contexts match pattern '%s'", name)
		}
		return matches, nil
	}
	// Exact match
	for _, ctx := range contexts {
		if ctx == name {
			return []string{ctx}, nil
		}
	}
	// Suffix/substring match — return ALL matches (useful for group add)
	var matches []string
	for _, ctx := range contexts {
		if strings.HasSuffix(ctx, "/"+name) || strings.Contains(ctx, name) {
			matches = append(matches, ctx)
		}
	}
	if len(matches) >= 1 {
		return matches, nil
	}
	return nil, fmt.Errorf("context '%s' not found", name)
}

func resolveContext(name string, contexts []string) (string, error) {
	results, err := resolveContexts(name, contexts)
	if err != nil {
		return "", err
	}
	if len(results) > 1 {
		return "", fmt.Errorf("ambiguous '%s', matches:\n  %s", name, strings.Join(results, "\n  "))
	}
	return results[0], nil
}

func handleGroup(cfg config) {
	if len(os.Args) < 3 {
		// No subcommand: list groups
		if len(cfg.Groups) == 0 {
			fmt.Println(dimStyle.Render("No groups configured. Use: ksw group add <name> [ctx...]"))
			return
		}
		names := make([]string, 0, len(cfg.Groups))
		for n := range cfg.Groups {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Printf("  %s %s %s\n", pinItemStyle.Render("◆"), aliasStyle.Render(n), dimStyle.Render(fmt.Sprintf("(%d contexts)", len(cfg.Groups[n]))))
		}
		return
	}

	sub := os.Args[2]

	switch sub {
	case "ls", "list":
		if len(cfg.Groups) == 0 {
			fmt.Println(dimStyle.Render("No groups configured. Use: ksw group add <name> [ctx...]"))
			return
		}
		names := make([]string, 0, len(cfg.Groups))
		for n := range cfg.Groups {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Printf("  %s %s\n", aliasStyle.Render(n), dimStyle.Render(fmt.Sprintf("(%d contexts)", len(cfg.Groups[n]))))
			for _, ctx := range cfg.Groups[n] {
				fmt.Printf("      %s %s\n", dimStyle.Render("·"), normalItemStyle.Render(ctx))
			}
		}

	case "add":
		// ksw group add <name> [ctx1 ctx2 ...]
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: ksw group add <name> [ctx...]")
			os.Exit(1)
		}
		groupName := os.Args[3]
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// Resolve any provided contexts (supports glob patterns like eks-sufi*)
		var resolved []string
		for _, arg := range os.Args[4:] {
			ctxs, err := resolveContexts(arg, contexts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s %v\n", warnStyle.Render("✗"), err)
				os.Exit(1)
			}
			for _, ctx := range ctxs {
				// Avoid duplicates
				found := false
				for _, r := range resolved {
					if r == ctx {
						found = true
						break
					}
				}
				if !found {
					resolved = append(resolved, ctx)
				}
			}
		}
		// Merge with existing group members
		existing := cfg.Groups[groupName]
		existingSet := make(map[string]bool, len(existing))
		for _, c := range existing {
			existingSet[c] = true
		}
		added := 0
		for _, ctx := range resolved {
			if !existingSet[ctx] {
				existing = append(existing, ctx)
				added++
			}
		}
		cfg.Groups[groupName] = existing
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		if len(resolved) == 0 {
			fmt.Printf("%s Created empty group %s\n", successStyle.Render("✔"), aliasStyle.Render(groupName))
			fmt.Printf("  Add contexts with: %s\n", dimStyle.Render("ksw group add-ctx "+groupName+" <ctx>"))
		} else if added == 0 {
			fmt.Printf("%s Group %s — already up to date (%d contexts)\n", dimStyle.Render("·"), aliasStyle.Render(groupName), len(cfg.Groups[groupName]))
		} else {
			fmt.Printf("%s Group %s — added %d context(s)\n", successStyle.Render("✔"), aliasStyle.Render(groupName), added)
			for _, ctx := range resolved {
				if !existingSet[ctx] {
					fmt.Printf("  %s %s\n", dimStyle.Render("·"), ctx)
				}
			}
		}

	case "rm", "remove":
		// ksw group rm <name> [name2 ...]
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: ksw group rm <name> [name2 ...]")
			os.Exit(1)
		}
		for _, groupName := range os.Args[3:] {
			if _, ok := cfg.Groups[groupName]; !ok {
				fmt.Fprintf(os.Stderr, "%s Group '%s' not found.\n", warnStyle.Render("✗"), groupName)
				continue
			}
			delete(cfg.Groups, groupName)
			fmt.Printf("%s Removed group %s\n", successStyle.Render("✔"), aliasStyle.Render(groupName))
		}
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}

	case "add-ctx":
		// ksw group add-ctx <group> <ctx>
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "Usage: ksw group add-ctx <group> <ctx>")
			os.Exit(1)
		}
		groupName := os.Args[3]
		if _, ok := cfg.Groups[groupName]; !ok {
			fmt.Fprintf(os.Stderr, "%s Group '%s' not found. Create it first with: ksw group add %s\n", warnStyle.Render("✗"), groupName, groupName)
			os.Exit(1)
		}
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		ctx, err := resolveContext(os.Args[4], contexts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", warnStyle.Render("✗"), err)
			os.Exit(1)
		}
		for _, c := range cfg.Groups[groupName] {
			if c == ctx {
				fmt.Printf("%s Already in group %s: %s\n", dimStyle.Render("·"), aliasStyle.Render(groupName), ctx)
				return
			}
		}
		cfg.Groups[groupName] = append(cfg.Groups[groupName], ctx)
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s Added to group %s: %s\n", successStyle.Render("✔"), aliasStyle.Render(groupName), ctx)

	case "rmi":
		// ksw group rmi <group> <ctx> [ctx2 ...]
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "Usage: ksw group rmi <group> <ctx> [ctx2 ...]")
			os.Exit(1)
		}
		groupName := os.Args[3]
		if _, ok := cfg.Groups[groupName]; !ok {
			fmt.Fprintf(os.Stderr, "%s Group '%s' not found.\n", warnStyle.Render("✗"), groupName)
			os.Exit(1)
		}
		// Build set of members to remove (supports substring and glob)
		toRemove := make(map[string]bool)
		for _, pattern := range os.Args[4:] {
			matched := false
			if strings.ContainsAny(pattern, "*?") {
				// Glob match
				for _, c := range cfg.Groups[groupName] {
					if globMatch(pattern, c) {
						toRemove[c] = true
						matched = true
					}
				}
				// Auto-wrap if no match
				if !matched && !strings.HasPrefix(pattern, "*") {
					wrapped := "*" + pattern
					for _, c := range cfg.Groups[groupName] {
						if globMatch(wrapped, c) {
							toRemove[c] = true
							matched = true
						}
					}
				}
			} else {
				// Exact, suffix or substring match
				for _, c := range cfg.Groups[groupName] {
					if c == pattern || strings.HasSuffix(c, "/"+pattern) || strings.Contains(c, pattern) {
						toRemove[c] = true
						matched = true
					}
				}
			}
			if !matched {
				fmt.Fprintf(os.Stderr, "%s '%s' not found in group '%s'.\n", warnStyle.Render("✗"), pattern, groupName)
			}
		}
		if len(toRemove) == 0 {
			os.Exit(1)
		}
		var newMembers []string
		for _, c := range cfg.Groups[groupName] {
			if !toRemove[c] {
				newMembers = append(newMembers, c)
			}
		}
		cfg.Groups[groupName] = newMembers
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		for c := range toRemove {
			fmt.Printf("%s Removed from group %s: %s\n", successStyle.Render("✔"), aliasStyle.Render(groupName), c)
		}

	case "use":
		// ksw group use <name> — open TUI filtered to group
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: ksw group use <name>")
			os.Exit(1)
		}
		groupName := os.Args[3]
		members, ok := cfg.Groups[groupName]
		if !ok {
			fmt.Fprintf(os.Stderr, "%s Group '%s' not found.\n", warnStyle.Render("✗"), groupName)
			os.Exit(1)
		}
		if len(members) == 0 {
			fmt.Fprintf(os.Stderr, "%s Group '%s' is empty.\n", warnStyle.Render("✗"), groupName)
			os.Exit(1)
		}
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		current := getCurrentContext()
		m := initialModel(contexts, current, cfg, groupName, false)
		p := tea.NewProgram(m, tea.WithAltScreen())
		result, err := p.Run()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		final := result.(model)
		if final.chosen != "" && final.chosen != current {
			recordHistory(&final.cfg, current, final.chosen)
			if err := switchContext(final.chosen); err != nil {
				fmt.Fprintf(os.Stderr, "Error switching to %s: %v\n", final.chosen, err)
				os.Exit(1)
			}
			_ = saveConfig(final.cfg)
			alias := final.aliasFor(final.chosen)
			extra := ""
			if alias != "" {
				extra = " " + aliasStyle.Render("@"+alias)
			}
			fmt.Printf("%s Switched to %s%s\n", successStyle.Render("✔"), final.chosen, extra)
		} else if final.chosen == current {
			fmt.Printf("%s Already on %s\n", dimStyle.Render("·"), current)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown group subcommand '%s'.\nUsage: ksw group <add|rm|ls|use|add-ctx|rmi>\n", sub)
		os.Exit(1)
	}
}

func handleAlias(cfg config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: ksw alias <ls|rm|name> [context]")
		os.Exit(1)
	}

	sub := os.Args[2]

	switch sub {
	case "ls", "list":
		if len(cfg.Aliases) == 0 {
			fmt.Println(dimStyle.Render("No aliases configured. Use: ksw alias <name> <context>"))
			return
		}
		// Sort aliases for consistent output
		names := make([]string, 0, len(cfg.Aliases))
		for name := range cfg.Aliases {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Printf("  %s → %s\n", aliasStyle.Render("@"+name), cfg.Aliases[name])
		}

	case "rm", "remove", "delete":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: ksw alias rm <name>")
			os.Exit(1)
		}
		name := os.Args[3]
		if _, ok := cfg.Aliases[name]; !ok {
			fmt.Fprintf(os.Stderr, "%s Alias '%s' not found.\n", warnStyle.Render("✗"), name)
			os.Exit(1)
		}
		delete(cfg.Aliases, name)
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s Removed alias %s\n", successStyle.Render("✔"), aliasStyle.Render("@"+name))

	default:
		// ksw alias <name> <context>
		name := sub
		if len(os.Args) < 4 {
			// Show what this alias points to
			if target, ok := cfg.Aliases[name]; ok {
				fmt.Printf("  %s → %s\n", aliasStyle.Render("@"+name), target)
			} else {
				fmt.Fprintf(os.Stderr, "Usage: ksw alias <name> <context>\n")
				os.Exit(1)
			}
			return
		}
		context := os.Args[3]
		cfg.Aliases[name] = context
		if err := saveConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s Alias %s → %s\n", successStyle.Render("✔"), aliasStyle.Render("@"+name), context)
	}
}
