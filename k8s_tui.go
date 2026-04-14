package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── K8s TUI types ──────────────────────────────────────

type k8sView int

const (
	k8sViewResources  k8sView = iota
	k8sViewList
	k8sViewDescribe
	k8sViewLogs
	k8sViewDelete
	k8sViewNamespaces
	k8sViewNsInput // manual namespace text input
)

type k8sResourceType struct {
	name, short, command string
}

var k8sResourceTypes = []k8sResourceType{
	{"Pods", "po", "pods"},
	{"Deployments", "deploy", "deployments"},
	{"Services", "svc", "services"},
	{"Ingresses", "ing", "ingresses"},
	{"ConfigMaps", "cm", "configmaps"},
	{"Secrets", "secret", "secrets"},
	{"Nodes", "node", "nodes"},
	{"Namespaces", "ns", "namespaces"},
	{"StatefulSets", "sts", "statefulsets"},
	{"DaemonSets", "ds", "daemonsets"},
	{"Jobs", "job", "jobs"},
	{"CronJobs", "cj", "cronjobs"},
	{"PersistentVolumeClaims", "pvc", "persistentvolumeclaims"},
	{"ReplicaSets", "rs", "replicasets"},
}

type k8sResource struct {
	name, status, age, ready, restarts, cpu, mem, ip, node string
	cpuReq, memReq, cpuLim, memLim                         string
	nodeIP                                                  string
	raw                                                     []string
}

// ── Messages ───────────────────────────────────────────

type (
	k8sResourcesLoadedMsg struct {
		resources []k8sResource
		headers   []string
		err       error
	}
	k8sPodResourcesLoadedMsg struct {
		// name → [cpuReq, memReq, cpuLim, memLim]
		resources map[string][4]string
	}
	k8sTopLoadedMsg struct {
		metrics map[string][2]string
	}
	k8sDescribeLoadedMsg struct {
		output string
		err    error
	}
	k8sLogsLoadedMsg struct {
		output string
		err    error
	}
	k8sDeleteDoneMsg struct {
		name string
		err  error
	}
	k8sNamespacesLoadedMsg struct {
		namespaces []string
		err        error
	}
	k8sSpinnerTickMsg struct{}
	k8sAutoRefreshMsg struct{}
	k8sTopRefreshMsg  struct{}
)

// ── Model ──────────────────────────────────────────────

type k8sModel struct {
	view      k8sView
	context   string
	namespace string

	resourceCursor int

	resources    []k8sResource
	headers      []string
	filtered     []int
	cachedCols   []k8sCol
	cachedWidths []int
	listCursor   int
	listScroll   int
	listHScroll  int // horizontal scroll for table columns
	search       string
	searching    bool // true when / search mode is active
	selectedType k8sResourceType
	lastRefresh  time.Time
	statusMsg    string

	selected map[int]bool // multi-select: set of filtered indices

	viewContent string
	viewScroll  int
	viewHScroll int // horizontal scroll offset
	viewLines   []string
	logSearch   string
	logSearching bool
	logMatches  []int
	logMatchSet map[int]bool
	logMatchIdx int

	deleteTargets []string

	namespaces []string
	nsCursor   int
	nsScroll   int
	nsSearch   string
	nsFiltered []int
	nsInput    string // manual namespace text input
	nsInputFrom k8sView // which view to return to

	loading      bool
	topLoading   bool
	spinnerFrame int

	width, height int
	quitting      bool
}

// ── Styles ─────────────────────────────────────────────

var (
	k8sTitleSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
	k8sOkSt     = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	k8sWarnSt   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	k8sErrSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	k8sDimSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	k8sSelSt    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#8be9fd"))
	k8sLabelSt  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f4a261"))
	k8sNormalSt = lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	k8sBarSt    = lipgloss.NewStyle().Foreground(lipgloss.Color("#44475a"))
	k8sHeaderSt = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#bd93f9")).Underline(true)

	// row styles by pod status
	k8sRowOk   = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	k8sRowWarn = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	k8sRowErr  = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	k8sRowDim  = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	k8sRowNorm = lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	k8sRowAlt  = lipgloss.NewStyle().Foreground(lipgloss.Color("#c8c8e0")) // zebra alternate row

	// describe/logs syntax
	k8sDescKey   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#8be9fd"))
	k8sDescVal   = lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	k8sDescLabel = lipgloss.NewStyle().Foreground(lipgloss.Color("#bd93f9"))
	k8sDescOk    = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	k8sDescWarn  = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	k8sDescErr   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	k8sDescDim   = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	k8sLogTs     = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	k8sLogErr    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	k8sLogWarn   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	k8sLogInfo   = lipgloss.NewStyle().Foreground(lipgloss.Color("#8be9fd"))

	// search match highlight
	k8sMatchHl     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#1a1a2e")).Background(lipgloss.Color("#f1fa8c"))
	k8sMatchLine   = lipgloss.NewStyle().Foreground(lipgloss.Color("#f1fa8c"))
	k8sMatchCurLine = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f1fa8c")).Background(lipgloss.Color("#44475a"))
)

var k8sSpinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// ── kubectl helpers ────────────────────────────────────

func k8sGetCurrentContext() string {
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func k8sGetCurrentNamespace() string {
	out, err := exec.Command("kubectl", "config", "view", "--minify", "--output", "jsonpath={.contexts[0].context.namespace}").Output()
	if err != nil || strings.TrimSpace(string(out)) == "" {
		return "default"
	}
	return strings.TrimSpace(string(out))
}

func k8sParseResources(output string) ([]k8sResource, []string) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		return nil, nil
	}

	// Parse header line using column positions instead of simple Fields split.
	// kubectl aligns columns with spaces; headers like "NOMINATED NODE" span
	// multiple words. We detect column start positions by finding runs of
	// non-space characters that are preceded by at least 2 spaces (or start of line).
	headerLine := lines[0]
	type colSpan struct {
		name       string
		start, end int // byte offsets in the line
	}
	var cols []colSpan
	i := 0
	for i < len(headerLine) {
		// skip whitespace
		for i < len(headerLine) && headerLine[i] == ' ' {
			i++
		}
		if i >= len(headerLine) {
			break
		}
		start := i
		// A column header ends when we see 2+ consecutive spaces or end of line.
		for i < len(headerLine) {
			if i+1 < len(headerLine) && headerLine[i] == ' ' && headerLine[i+1] == ' ' {
				break
			}
			i++
		}
		name := strings.TrimSpace(headerLine[start:i])
		if name != "" {
			cols = append(cols, colSpan{name: name, start: start, end: i})
		}
	}

	// Build simple header list for compatibility
	headers := make([]string, len(cols))
	for ci, col := range cols {
		headers[ci] = col.name
	}

	var resources []k8sResource
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		r := k8sResource{raw: strings.Fields(line)}
		if len(r.raw) > 0 {
			r.name = r.raw[0]
		}
		for ci, col := range cols {
			// extract value at the column position
			s := col.start
			var e int
			if ci+1 < len(cols) {
				e = cols[ci+1].start
			} else {
				e = len(line)
			}
			if s >= len(line) {
				continue
			}
			if e > len(line) {
				e = len(line)
			}
			val := strings.TrimSpace(line[s:e])

			switch strings.ToUpper(col.name) {
			case "STATUS", "PHASE":
				r.status = val
			case "AGE":
				r.age = val
			case "READY":
				r.ready = val
			case "RESTARTS":
				r.restarts = val
			case "IP":
				r.ip = val
			case "NODE":
				r.node = val
			case "NAME":
				r.name = val
			}
		}
		resources = append(resources, r)
	}
	return resources, headers
}

func k8sLoadResources(rt, ns string) tea.Cmd {
	return func() tea.Msg {
		args := []string{"get", rt, "-o", "wide", "--no-headers=false", "--request-timeout=8s"}
		if rt != "nodes" && rt != "namespaces" {
			args = append(args, "-n", ns)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sResourcesLoadedMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		res, hdr := k8sParseResources(string(out))
		return k8sResourcesLoadedMsg{resources: res, headers: hdr}
	}
}

func k8sLoadTop(rt, ns string) tea.Cmd {
	return func() tea.Msg {
		topType := ""
		if rt == "pods" {
			topType = "pods"
		} else if rt == "nodes" {
			topType = "nodes"
		} else {
			return k8sTopLoadedMsg{}
		}
		args := []string{"top", topType, "--no-headers", "--request-timeout=4s"}
		if topType == "pods" {
			args = append(args, "-n", ns)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sTopLoadedMsg{}
		}
		m := make(map[string][2]string)
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(line)
			if len(f) >= 3 {
				m[f[0]] = [2]string{f[1], f[2]}
			}
		}
		return k8sTopLoadedMsg{metrics: m}
	}
}

func k8sLoadPodResources(ns string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", "get", "pods", "-n", ns, "-o",
			`jsonpath={range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.resources.requests.cpu}{","}{.resources.requests.memory}{","}{.resources.limits.cpu}{","}{.resources.limits.memory}{" "}{end}{"\n"}{end}`,
		).Output()
		if err != nil {
			return k8sPodResourcesLoadedMsg{}
		}
		m := make(map[string][4]string)
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				continue
			}
			name := parts[0]
			// take first container's resources
			first := strings.Fields(parts[1])
			if len(first) == 0 {
				continue
			}
			fields := strings.Split(first[0], ",")
			if len(fields) == 4 {
				m[name] = [4]string{fields[0], fields[1], fields[2], fields[3]}
			}
		}
		return k8sPodResourcesLoadedMsg{resources: m}
	}
}

func k8sDescribe(rt, name, ns string) tea.Cmd {
	return func() tea.Msg {
		args := []string{"describe", rt, name}
		if rt != "nodes" && rt != "namespaces" {
			args = append(args, "-n", ns)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sDescribeLoadedMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		return k8sDescribeLoadedMsg{output: string(out)}
	}
}

func k8sGetLogs(name, ns string, tail int) tea.Cmd {
	return func() tea.Msg {
		args := []string{"logs", name, "-n", ns, "--tail", fmt.Sprintf("%d", tail)}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sLogsLoadedMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		return k8sLogsLoadedMsg{output: string(out)}
	}
}

func k8sGetLogsByLabel(label, ns string, tail int) tea.Cmd {
	return func() tea.Msg {
		args := []string{"logs", "-l", label, "-n", ns, "--tail", fmt.Sprintf("%d", tail), "--prefix", "--all-containers=true"}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sLogsLoadedMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		return k8sLogsLoadedMsg{output: string(out)}
	}
}

func k8sDeleteResource(rt string, names []string, ns string) tea.Cmd {
	return func() tea.Msg {
		args := []string{"delete", rt}
		args = append(args, names...)
		if rt != "nodes" && rt != "namespaces" {
			args = append(args, "-n", ns)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
		if err != nil {
			return k8sDeleteDoneMsg{name: strings.Join(names, ", "), err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		return k8sDeleteDoneMsg{name: strings.Join(names, ", ")}
	}
}

func k8sLoadNamespaces() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "kubectl", "get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}").Output()
		if err != nil {
			return k8sNamespacesLoadedMsg{err: err}
		}
		ns := strings.Fields(strings.TrimSpace(string(out)))
		sort.Strings(ns)
		return k8sNamespacesLoadedMsg{namespaces: ns}
	}
}

func k8sSpinnerTick() tea.Cmd {
	return tea.Tick(80*time.Millisecond, func(_ time.Time) tea.Msg { return k8sSpinnerTickMsg{} })
}

func k8sAutoRefreshCmd() tea.Cmd {
	return tea.Tick(30*time.Second, func(_ time.Time) tea.Msg { return k8sAutoRefreshMsg{} })
}

func k8sTopRefreshCmd() tea.Cmd {
	return tea.Tick(2*time.Minute, func(_ time.Time) tea.Msg { return k8sTopRefreshMsg{} })
}

// ── Status classification ──────────────────────────────

type k8sStatusClass int

const (
	k8sClassOk k8sStatusClass = iota
	k8sClassWarn
	k8sClassErr
	k8sClassDim
	k8sClassNorm
)

func k8sClassifyStatus(s string) k8sStatusClass {
	low := strings.ToLower(s)
	switch {
	case low == "running" || low == "active" || low == "ready" || low == "available" ||
		low == "bound" || low == "complete" || low == "succeeded" || low == "true":
		return k8sClassOk
	case low == "pending" || low == "containercreating" || low == "waiting" ||
		strings.HasPrefix(low, "init:"):
		return k8sClassWarn
	case low == "failed" || low == "error" || low == "crashloopbackoff" ||
		low == "imagepullbackoff" || low == "evicted" || low == "oomkilled" ||
		low == "errimagepull" || low == "invalidimgname" || low == "false":
		return k8sClassErr
	case low == "terminating" || low == "completed":
		return k8sClassDim
	default:
		return k8sClassNorm
	}
}

func k8sRowStyleFor(cls k8sStatusClass) lipgloss.Style {
	switch cls {
	case k8sClassOk:
		return k8sRowOk
	case k8sClassWarn:
		return k8sRowWarn
	case k8sClassErr:
		return k8sRowErr
	case k8sClassDim:
		return k8sRowDim
	default:
		return k8sRowNorm
	}
}

// ── Dynamic column calculation ─────────────────────────

type k8sCol struct {
	header string
	values []string // one per filtered resource
}

// k8sBuildColumns builds columns for the given resource type.
// availW is the usable width; optional columns are dropped if they don't fit.
func (m k8sModel) k8sBuildColumns(availW int) []k8sCol {
	n := len(m.filtered)
	getAll := func(fn func(k8sResource) string) []string {
		vals := make([]string, n)
		for i, idx := range m.filtered {
			if idx < len(m.resources) {
				vals[i] = fn(m.resources[idx])
			}
		}
		return vals
	}

	// colWidth returns the natural width (header or longest value + 2 gap)
	colWidth := func(header string, vals []string) int {
		w := utf8.RuneCountInString(header)
		for _, v := range vals {
			if vw := utf8.RuneCountInString(v); vw > w {
				w = vw
			}
		}
		return w + 2
	}

	// fits checks if adding a column still leaves space for NAME
	usedW := 0
	fits := func(header string, vals []string) bool {
		return usedW+colWidth(header, vals) < availW
	}

	switch m.selectedType.command {
	case "pods":
		nodes := getAll(func(r k8sResource) string { return r.node })
		for i, nd := range nodes {
			if dot := strings.Index(nd, "."); dot > 0 {
				nodes[i] = nd[:dot]
			}
		}
		ips := getAll(func(r k8sResource) string { return r.ip })
		cpus := getAll(func(r k8sResource) string { return r.cpu })
		mems := getAll(func(r k8sResource) string { return r.mem })
		cpuReqs := getAll(func(r k8sResource) string { return r.cpuReq })
		memReqs := getAll(func(r k8sResource) string { return r.memReq })
		cpuLims := getAll(func(r k8sResource) string { return r.cpuLim })
		memLims := getAll(func(r k8sResource) string { return r.memLim })
		readys := getAll(func(r k8sResource) string { return r.ready })
		statuses := getAll(func(r k8sResource) string { return r.status })
		restarts := getAll(func(r k8sResource) string { return r.restarts })
		ages := getAll(func(r k8sResource) string { return r.age })

		// calculate NAME natural width so we can reserve space for it
		names := getAll(func(r k8sResource) string { return r.name })
		nameW := colWidth("NAME", names)

		// fixed columns always shown
		fixed := []k8sCol{
			{"READY", readys},
			{"STATUS", statuses},
			{"RESTARTS", restarts},
			{"AGE", ages},
		}
		for _, c := range fixed {
			usedW += colWidth(c.header, c.values)
		}

		// optional columns: evaluated in priority order (first = highest priority = claims space first)
		// High priority: IP, NODE, CPU, MEM — Low priority: resource requests/limits
		type optCol struct {
			h string
			v []string
		}
		optionals := []optCol{
			{"IP", ips},
			{"NODE", nodes},
			{"CPU", cpus},
			{"MEM", mems},
			{"CPU_REQ", cpuReqs},
			{"MEM_REQ", memReqs},
			{"CPU_LIM", cpuLims},
			{"MEM_LIM", memLims},
		}
		// fitsWithName checks if adding a column still leaves enough space for NAME
		var optional []k8sCol
		for _, o := range optionals {
			cw := colWidth(o.h, o.v)
			if usedW+cw+nameW <= availW {
				usedW += cw
				optional = append(optional, k8sCol{o.h, o.v})
			}
		}

		cols := []k8sCol{{"NAME", names}}
		cols = append(cols, fixed...)
		cols = append(cols, optional...)
		return cols

	case "nodes":
		cpus := getAll(func(r k8sResource) string { return r.cpu })
		mems := getAll(func(r k8sResource) string { return r.mem })
		statuses := getAll(func(r k8sResource) string { return r.status })
		ages := getAll(func(r k8sResource) string { return r.age })
		fixed := []k8sCol{{"STATUS", statuses}, {"AGE", ages}}
		for _, c := range fixed {
			usedW += colWidth(c.header, c.values)
		}
		var optional []k8sCol
		if fits("MEM", mems) {
			usedW += colWidth("MEM", mems)
			optional = append([]k8sCol{{"MEM", mems}}, optional...)
		}
		if fits("CPU", cpus) {
			usedW += colWidth("CPU", cpus)
			optional = append([]k8sCol{{"CPU", cpus}}, optional...)
		}
		cols := []k8sCol{{"NAME", getAll(func(r k8sResource) string { return r.name })}}
		cols = append(cols, fixed...)
		cols = append(cols, optional...)
		return cols

	case "services":
		ports := getAll(func(r k8sResource) string {
			if len(r.raw) > 4 {
				return r.raw[4]
			}
			return ""
		})
		for i, p := range ports {
			if utf8.RuneCountInString(p) > 30 {
				ports[i] = string([]rune(p)[:28]) + "…"
			}
		}
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"TYPE", getAll(func(r k8sResource) string {
				if len(r.raw) > 1 {
					return r.raw[1]
				}
				return ""
			})},
			{"CLUSTER-IP", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"PORT(S)", ports},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	case "deployments", "statefulsets", "daemonsets", "replicasets":
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"READY", getAll(func(r k8sResource) string { return r.ready })},
			{"UP-TO-DATE", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"AVAILABLE", getAll(func(r k8sResource) string {
				if len(r.raw) > 3 {
					return r.raw[3]
				}
				return ""
			})},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	case "ingresses":
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"CLASS", getAll(func(r k8sResource) string {
				if len(r.raw) > 1 {
					return r.raw[1]
				}
				return ""
			})},
			{"HOSTS", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"ADDRESS", getAll(func(r k8sResource) string {
				if len(r.raw) > 3 {
					return r.raw[3]
				}
				return ""
			})},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	default:
		cols := []k8sCol{{"NAME", getAll(func(r k8sResource) string { return r.name })}}
		if hasField(m.resources, m.filtered, func(r k8sResource) string { return r.status }) {
			cols = append(cols, k8sCol{"STATUS", getAll(func(r k8sResource) string { return r.status })})
		}
		cols = append(cols, k8sCol{"AGE", getAll(func(r k8sResource) string { return r.age })})
		return cols
	}
}

// k8sBuildAllColumns returns ALL columns for the current resource type without
// any width limit. Used by the horizontal-scroll renderer in viewList.
func (m k8sModel) k8sBuildAllColumns() []k8sCol {
	n := len(m.filtered)
	getAll := func(fn func(k8sResource) string) []string {
		vals := make([]string, n)
		for i, idx := range m.filtered {
			if idx < len(m.resources) {
				vals[i] = fn(m.resources[idx])
			}
		}
		return vals
	}

	switch m.selectedType.command {
	case "pods":
		nodes := getAll(func(r k8sResource) string { return r.node })
		for i, nd := range nodes {
			if dot := strings.Index(nd, "."); dot > 0 {
				nodes[i] = nd[:dot]
			}
		}
		// fixed columns
		cols := []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"READY", getAll(func(r k8sResource) string { return r.ready })},
			{"STATUS", getAll(func(r k8sResource) string { return r.status })},
			{"RESTARTS", getAll(func(r k8sResource) string { return r.restarts })},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
			{"IP", getAll(func(r k8sResource) string { return r.ip })},
			{"NODE", nodes},
			{"CPU", getAll(func(r k8sResource) string { return r.cpu })},
			{"MEM", getAll(func(r k8sResource) string { return r.mem })},
			{"CPU_REQ", getAll(func(r k8sResource) string { return r.cpuReq })},
			{"MEM_REQ", getAll(func(r k8sResource) string { return r.memReq })},
			{"CPU_LIM", getAll(func(r k8sResource) string { return r.cpuLim })},
			{"MEM_LIM", getAll(func(r k8sResource) string { return r.memLim })},
		}
		return cols

	case "nodes":
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"STATUS", getAll(func(r k8sResource) string { return r.status })},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
			{"CPU", getAll(func(r k8sResource) string { return r.cpu })},
			{"MEM", getAll(func(r k8sResource) string { return r.mem })},
		}

	case "services":
		ports := getAll(func(r k8sResource) string {
			if len(r.raw) > 4 {
				return r.raw[4]
			}
			return ""
		})
		for i, p := range ports {
			if utf8.RuneCountInString(p) > 30 {
				ports[i] = string([]rune(p)[:28]) + "…"
			}
		}
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"TYPE", getAll(func(r k8sResource) string {
				if len(r.raw) > 1 {
					return r.raw[1]
				}
				return ""
			})},
			{"CLUSTER-IP", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"PORT(S)", ports},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	case "deployments", "statefulsets", "daemonsets", "replicasets":
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"READY", getAll(func(r k8sResource) string { return r.ready })},
			{"UP-TO-DATE", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"AVAILABLE", getAll(func(r k8sResource) string {
				if len(r.raw) > 3 {
					return r.raw[3]
				}
				return ""
			})},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	case "ingresses":
		return []k8sCol{
			{"NAME", getAll(func(r k8sResource) string { return r.name })},
			{"CLASS", getAll(func(r k8sResource) string {
				if len(r.raw) > 1 {
					return r.raw[1]
				}
				return ""
			})},
			{"HOSTS", getAll(func(r k8sResource) string {
				if len(r.raw) > 2 {
					return r.raw[2]
				}
				return ""
			})},
			{"ADDRESS", getAll(func(r k8sResource) string {
				if len(r.raw) > 3 {
					return r.raw[3]
				}
				return ""
			})},
			{"AGE", getAll(func(r k8sResource) string { return r.age })},
		}

	default:
		cols := []k8sCol{{"NAME", getAll(func(r k8sResource) string { return r.name })}}
		if hasField(m.resources, m.filtered, func(r k8sResource) string { return r.status }) {
			cols = append(cols, k8sCol{"STATUS", getAll(func(r k8sResource) string { return r.status })})
		}
		cols = append(cols, k8sCol{"AGE", getAll(func(r k8sResource) string { return r.age })})
		return cols
	}
}

func hasField(res []k8sResource, filtered []int, fn func(k8sResource) string) bool {
	for _, idx := range filtered {
		if idx < len(res) && fn(res[idx]) != "" {
			return true
		}
	}
	return false
}

// k8sCalcWidths calculates column widths based on actual content, fitting within maxW.
// Non-name columns get their exact needed width + gap. NAME gets the remaining space.
func k8sCalcWidths(cols []k8sCol, maxW int) []int {
	if len(cols) == 0 {
		return nil
	}
	gap := 2 // space between columns
	widths := make([]int, len(cols))

	// first pass: each column gets max(header, longest value) + gap
	for i, c := range cols {
		w := utf8.RuneCountInString(c.header)
		for _, v := range c.values {
			vw := utf8.RuneCountInString(v)
			if vw > w {
				w = vw
			}
		}
		widths[i] = w + gap
	}

	// check total
	total := 0
	for _, w := range widths {
		total += w
	}

	if total <= maxW {
		// fits — give leftover space to NAME column (index 0) so table fills the width
		widths[0] += maxW - total
		return widths
	}

	// doesn't fit: shrink only NAME column (index 0), keep others at natural width
	otherTotal := 0
	for i := 1; i < len(widths); i++ {
		otherTotal += widths[i]
	}
	nameW := maxW - otherTotal
	minName := utf8.RuneCountInString(cols[0].header) + gap
	if nameW < minName {
		nameW = minName
	}
	widths[0] = nameW

	return widths
}

func k8sPad(s string, w int) string {
	vis := utf8.RuneCountInString(s)
	if vis >= w {
		// truncate by runes
		if w > 2 {
			runes := []rune(s)
			return string(runes[:w-2]) + "… "
		}
		runes := []rune(s)
		return string(runes[:w])
	}
	return s + strings.Repeat(" ", w-vis)
}

// k8sStyleCell applies semantic color to a cell based on its column header and value.
func k8sStyleCell(header, val, padded string, w int, isCursor, isAlt bool) string {
	if isCursor {
		return k8sSelSt.Render(padded)
	}

	empty := val == "" || val == "<none>"

	switch header {
	case "NAME":
		if isAlt {
			return k8sRowAlt.Render(padded)
		}
		return k8sNormalSt.Render(padded)

	case "STATUS", "PHASE":
		cls := k8sClassifyStatus(val)
		return k8sRowStyleFor(cls).Render(padded)

	case "READY":
		// color based on ready ratio: "3/3" = green, "1/3" = warn
		parts := strings.SplitN(val, "/", 2)
		if len(parts) == 2 && parts[0] == parts[1] {
			return k8sOkSt.Render(padded)
		} else if len(parts) == 2 && parts[0] != "0" {
			return k8sWarnSt.Render(padded)
		} else if len(parts) == 2 && parts[0] == "0" {
			return k8sErrSt.Render(padded)
		}
		if isAlt {
			return k8sRowAlt.Render(padded)
		}
		return k8sNormalSt.Render(padded)

	case "RESTARTS":
		if val != "" && val != "0" {
			return k8sWarnSt.Render(padded)
		}
		return k8sDimSt.Render(padded)

	case "AGE":
		return k8sDimSt.Render(padded)

	case "CPU", "MEM":
		if empty {
			return k8sDimSt.Render(k8sPad("-", w))
		}
		return k8sLabelSt.Render(padded)

	case "CPU_REQ", "MEM_REQ":
		if empty {
			return k8sDimSt.Render(k8sPad("-", w))
		}
		return k8sOkSt.Render(padded)

	case "CPU_LIM", "MEM_LIM":
		if empty {
			return k8sDimSt.Render(k8sPad("-", w))
		}
		return k8sWarnSt.Render(padded)

	case "IP", "NODE":
		if empty {
			return k8sDimSt.Render(k8sPad("-", w))
		}
		return k8sDimSt.Render(padded)

	case "TYPE":
		// service type coloring
		switch val {
		case "ClusterIP":
			return k8sDimSt.Render(padded)
		case "NodePort":
			return k8sWarnSt.Render(padded)
		case "LoadBalancer":
			return k8sOkSt.Render(padded)
		case "ExternalName":
			return k8sLabelSt.Render(padded)
		}
		if isAlt {
			return k8sRowAlt.Render(padded)
		}
		return k8sNormalSt.Render(padded)

	case "UP-TO-DATE", "AVAILABLE":
		if isAlt {
			return k8sRowAlt.Render(padded)
		}
		return k8sNormalSt.Render(padded)

	case "CLUSTER-IP":
		return k8sDimSt.Render(padded)

	case "PORT(S)":
		return k8sLabelSt.Render(padded)

	default:
		if isAlt {
			return k8sRowAlt.Render(padded)
		}
		return k8sNormalSt.Render(padded)
	}
}

// ── Describe/Logs syntax highlighting ──────────────────

func k8sHighlightDescribe(line string) string {
	trimmed := strings.TrimSpace(line)
	indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]

	// section headers (no indent, ends with colon)
	if len(indent) == 0 && strings.HasSuffix(trimmed, ":") && !strings.Contains(trimmed, " ") {
		return k8sDescKey.Bold(true).Render(line)
	}

	// key: value lines
	if idx := strings.Index(trimmed, ":"); idx > 0 && idx < len(trimmed)-1 {
		key := trimmed[:idx]
		val := strings.TrimSpace(trimmed[idx+1:])
		// only treat as key:value if key has no spaces (or is a known pattern)
		if !strings.Contains(key, " ") || strings.HasPrefix(key, "  ") {
			styledVal := k8sHighlightValue(val)
			return indent + k8sDescKey.Render(key+":") + " " + styledVal
		}
	}

	// key with just colon at end (subsection)
	if strings.HasSuffix(trimmed, ":") {
		return indent + k8sDescLabel.Render(trimmed)
	}

	// lines with status keywords
	low := strings.ToLower(trimmed)
	if strings.Contains(low, "error") || strings.Contains(low, "failed") ||
		strings.Contains(low, "crashloopbackoff") || strings.Contains(low, "oomkilled") {
		return indent + k8sDescErr.Render(trimmed)
	}
	if strings.Contains(low, "warning") || strings.Contains(low, "backoff") {
		return indent + k8sDescWarn.Render(trimmed)
	}
	if strings.Contains(low, "running") || strings.Contains(low, "ready") ||
		strings.Contains(low, "started") || strings.Contains(low, "normal") ||
		strings.Contains(low, "success") || strings.Contains(low, "true") {
		return indent + k8sDescOk.Render(trimmed)
	}

	// annotations/labels (key=value)
	if strings.Contains(trimmed, "=") && !strings.Contains(trimmed, " ") {
		parts := strings.SplitN(trimmed, "=", 2)
		return indent + k8sDescLabel.Render(parts[0]) + k8sDescDim.Render("=") + k8sDescVal.Render(parts[1])
	}

	return indent + k8sDescVal.Render(trimmed)
}

func k8sHighlightValue(val string) string {
	low := strings.ToLower(val)
	switch {
	case low == "true" || low == "running" || low == "ready" || low == "active" || low == "succeeded":
		return k8sDescOk.Render(val)
	case low == "false" || low == "failed" || low == "error" || low == "crashloopbackoff":
		return k8sDescErr.Render(val)
	case low == "pending" || low == "waiting" || low == "unknown" || strings.HasPrefix(low, "init:"):
		return k8sDescWarn.Render(val)
	case low == "<none>" || low == "<nil>" || low == "<unset>":
		return k8sDescDim.Render(val)
	default:
		return k8sDescVal.Render(val)
	}
}

func k8sHighlightLog(line string) string {
	if len(line) == 0 {
		return ""
	}

	// try to detect timestamp prefix (ISO 8601 or common formats)
	tsEnd := -1
	if len(line) > 24 && (line[4] == '-' || line[4] == '/') {
		// looks like 2024-01-15T10:30:00.000Z or similar
		for i, c := range line {
			if c == ' ' || c == '\t' {
				tsEnd = i
				break
			}
			if i > 35 {
				break
			}
		}
	}

	styled := line
	if tsEnd > 0 {
		ts := line[:tsEnd]
		rest := line[tsEnd:]
		styled = k8sLogTs.Render(ts) + k8sHighlightLogContent(rest)
	} else {
		styled = k8sHighlightLogContent(line)
	}
	return styled
}

func k8sHighlightLogContent(s string) string {
	low := strings.ToLower(s)
	switch {
	case strings.Contains(low, "error") || strings.Contains(low, "fatal") ||
		strings.Contains(low, "panic") || strings.Contains(low, "exception"):
		return k8sLogErr.Render(s)
	case strings.Contains(low, "warn") || strings.Contains(low, "timeout") ||
		strings.Contains(low, "deprecated"):
		return k8sLogWarn.Render(s)
	case strings.Contains(low, "info") || strings.Contains(low, "debug"):
		return k8sLogInfo.Render(s)
	default:
		return k8sNormalSt.Render(s)
	}
}

// k8sHighlightMatch highlights all occurrences of needle in line with a bright background
func k8sHighlightMatch(line, needle string) string {
	if needle == "" {
		return k8sNormalSt.Render(line)
	}
	// determine the base style from log level
	baseSt := k8sNormalSt
	low := strings.ToLower(line)
	switch {
	case strings.Contains(low, "error") || strings.Contains(low, "fatal") ||
		strings.Contains(low, "panic") || strings.Contains(low, "exception"):
		baseSt = k8sLogErr
	case strings.Contains(low, "warn") || strings.Contains(low, "timeout") ||
		strings.Contains(low, "deprecated"):
		baseSt = k8sLogWarn
	case strings.Contains(low, "info") || strings.Contains(low, "debug"):
		baseSt = k8sLogInfo
	}

	nlow := strings.ToLower(needle)
	var b strings.Builder
	pos := 0
	for {
		idx := strings.Index(strings.ToLower(line[pos:]), nlow)
		if idx < 0 {
			b.WriteString(baseSt.Render(line[pos:]))
			break
		}
		// text before match
		if idx > 0 {
			b.WriteString(baseSt.Render(line[pos : pos+idx]))
		}
		// the match itself
		b.WriteString(k8sMatchHl.Render(line[pos+idx : pos+idx+len(needle)]))
		pos += idx + len(needle)
	}
	return b.String()
}

// ── Init / Update ──────────────────────────────────────

func (m k8sModel) Init() tea.Cmd { return tea.WindowSize() }

func (m k8sModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		// invalidate column cache so it rebuilds with new width
		m.cachedCols = nil
		m.cachedWidths = nil
		return m, nil

	case k8sSpinnerTickMsg:
		if m.loading {
			m.spinnerFrame = (m.spinnerFrame + 1) % len(k8sSpinner)
			return m, k8sSpinnerTick()
		}
		return m, nil

	case k8sAutoRefreshMsg:
		if m.view == k8sViewList && !m.loading && m.selectedType.command != "" {
			return m, tea.Batch(
				k8sLoadResources(m.selectedType.command, m.namespace),
				k8sAutoRefreshCmd(),
			)
		}
		if m.view == k8sViewList {
			return m, k8sAutoRefreshCmd()
		}
		return m, nil

	case k8sTopRefreshMsg:
		if m.view == k8sViewList && !m.topLoading && m.selectedType.command != "" {
			m.topLoading = true
			return m, tea.Batch(k8sLoadTop(m.selectedType.command, m.namespace), k8sTopRefreshCmd())
		}
		return m, k8sTopRefreshCmd()

	case k8sResourcesLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.resources = nil
			m.headers = nil
			m.filtered = nil
			m.statusMsg = msg.err.Error()
			return m, k8sAutoRefreshCmd()
		}
		m.statusMsg = ""
		prevName := ""
		if len(m.resources) > 0 && len(m.filtered) > 0 && m.listCursor < len(m.filtered) && m.filtered[m.listCursor] < len(m.resources) {
			prevName = m.resources[m.filtered[m.listCursor]].name
		}
		// preserve static pod resource data (requests/limits) and top metrics across refreshes
		prevResources := make(map[string][4]string, len(m.resources))
		prevTop := make(map[string][2]string, len(m.resources))
		for _, r := range m.resources {
			if r.cpuReq != "" || r.memReq != "" {
				prevResources[r.name] = [4]string{r.cpuReq, r.memReq, r.cpuLim, r.memLim}
			}
			if r.cpu != "" || r.mem != "" {
				prevTop[r.name] = [2]string{r.cpu, r.mem}
			}
		}
		m.resources = msg.resources
		m.headers = msg.headers
		// restore preserved resource data
		for i, r := range m.resources {
			if prev, ok := prevResources[r.name]; ok {
				m.resources[i].cpuReq = prev[0]
				m.resources[i].memReq = prev[1]
				m.resources[i].cpuLim = prev[2]
				m.resources[i].memLim = prev[3]
			}
			if top, ok := prevTop[r.name]; ok {
				m.resources[i].cpu = top[0]
				m.resources[i].mem = top[1]
			}
		}
		m.lastRefresh = time.Now()
		m.k8sApplyFilter()
		if prevName != "" {
			for i, idx := range m.filtered {
				if idx < len(m.resources) && m.resources[idx].name == prevName {
					m.listCursor = i
					break
				}
			}
		}
		m.ensureListVisible()
		// rebuild column cache
		m.cachedCols = m.k8sBuildColumns(m.width - 4)
		m.cachedWidths = k8sCalcWidths(m.cachedCols, m.width-4)
		return m, nil

	case k8sTopLoadedMsg:
		m.topLoading = false
		if msg.metrics != nil {
			for i, r := range m.resources {
				if met, ok := msg.metrics[r.name]; ok {
					m.resources[i].cpu = met[0]
					m.resources[i].mem = met[1]
				}
			}
			// refresh column cache with new metrics
			m.cachedCols = m.k8sBuildColumns(m.width - 4)
			if m.width > 0 {
				m.cachedWidths = k8sCalcWidths(m.cachedCols, m.width-4)
			}
		}
		return m, nil

	case k8sPodResourcesLoadedMsg:
		if msg.resources != nil {
			for i, r := range m.resources {
				if res, ok := msg.resources[r.name]; ok {
					m.resources[i].cpuReq = res[0]
					m.resources[i].memReq = res[1]
					m.resources[i].cpuLim = res[2]
					m.resources[i].memLim = res[3]
				}
			}
			m.cachedCols = m.k8sBuildColumns(m.width - 4)
			if m.width > 0 {
				m.cachedWidths = k8sCalcWidths(m.cachedCols, m.width-4)
			}
		}
		return m, nil

	case k8sDescribeLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.viewContent = msg.err.Error()
		} else {
			m.viewContent = msg.output
		}
		m.viewLines = strings.Split(m.viewContent, "\n")
		m.viewScroll = 0
		m.viewHScroll = 0
		m.view = k8sViewDescribe
		return m, nil

	case k8sLogsLoadedMsg:
		m.loading = false
		if msg.err != nil {
			m.viewContent = msg.err.Error()
		} else {
			m.viewContent = msg.output
		}
		m.viewLines = strings.Split(m.viewContent, "\n")
		m.viewScroll = 0
		m.viewHScroll = 0
		m.view = k8sViewLogs
		return m, nil

	case k8sDeleteDoneMsg:
		m.loading = false
		if msg.err != nil {
			m.statusMsg = "✗ " + msg.err.Error()
		} else {
			m.statusMsg = "✔ Deleted " + msg.name
		}
		m.view = k8sViewList
		// clear cached data so it fully reloads
		m.resources = nil
		cmds := []tea.Cmd{
			k8sLoadResources(m.selectedType.command, m.namespace),
			k8sLoadTop(m.selectedType.command, m.namespace),
			k8sAutoRefreshCmd(),
		}
		if m.selectedType.command == "pods" {
			cmds = append(cmds, k8sLoadPodResources(m.namespace))
		}
		return m, tea.Batch(cmds...)

	case k8sNamespacesLoadedMsg:
		m.loading = false
		if msg.err != nil || len(msg.namespaces) == 0 {
			// no permission or empty — fall back to manual input
			m.nsInput = m.namespace
			m.view = k8sViewNsInput
			return m, nil
		}
		m.namespaces = msg.namespaces
		m.nsCursor = 0
		m.nsScroll = 0
		m.nsSearch = ""
		m.k8sApplyNsFilter()
		m.view = k8sViewNamespaces
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *k8sModel) k8sApplyFilter() {
	m.filtered = nil
	for i, r := range m.resources {
		if m.search == "" || strings.Contains(strings.ToLower(r.name), strings.ToLower(m.search)) ||
			strings.Contains(strings.ToLower(r.status), strings.ToLower(m.search)) {
			m.filtered = append(m.filtered, i)
		}
	}
	m.cachedCols = nil
	m.cachedWidths = nil
}

func (m *k8sModel) k8sApplyNsFilter() {
	m.nsFiltered = nil
	for i, ns := range m.namespaces {
		if m.nsSearch == "" || strings.Contains(strings.ToLower(ns), strings.ToLower(m.nsSearch)) {
			m.nsFiltered = append(m.nsFiltered, i)
		}
	}
}

func (m *k8sModel) ensureListVisible() {
	if len(m.filtered) == 0 {
		m.listCursor = 0
		m.listScroll = 0
		return
	}
	if m.listCursor >= len(m.filtered) {
		m.listCursor = len(m.filtered) - 1
	}
	maxVis := m.maxListVisible()
	if m.listCursor < m.listScroll {
		m.listScroll = m.listCursor
	}
	if m.listCursor >= m.listScroll+maxVis {
		m.listScroll = m.listCursor - maxVis + 1
	}
}

func (m *k8sModel) ensureNsVisible() {
	maxVis := m.height - 10
	if maxVis < 5 {
		maxVis = 5
	}
	if m.nsCursor < m.nsScroll {
		m.nsScroll = m.nsCursor
	}
	if m.nsCursor >= m.nsScroll+maxVis {
		m.nsScroll = m.nsCursor - maxVis + 1
	}
}

func (m k8sModel) maxListVisible() int {
	// header(1) + topbar(1) + title(1) + colheader(1) + footer(2) + bottombar(1) = 7
	v := m.height - 7
	if m.searching || m.search != "" {
		v-- // search bar takes a line
	}
	if m.statusMsg != "" {
		v-- // status msg takes a line
	}
	if v < 3 {
		v = 3
	}
	return v
}

// ── Key handling ───────────────────────────────────────

func (m k8sModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	if key == "ctrl+c" {
		m.quitting = true
		return m, tea.Quit
	}
	switch m.view {
	case k8sViewResources:
		return m.handleResourcesKey(key)
	case k8sViewList:
		return m.handleListKey(key)
	case k8sViewDescribe, k8sViewLogs:
		return m.handleViewerKey(key)
	case k8sViewDelete:
		return m.handleDeleteKey(key)
	case k8sViewNamespaces:
		return m.handleNamespacesKey(key)
	case k8sViewNsInput:
		return m.handleNsInputKey(key)
	}
	return m, nil
}

func (m k8sModel) handleResourcesKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "up", "k":
		if m.resourceCursor > 0 {
			m.resourceCursor--
		}
	case "down", "j":
		if m.resourceCursor < len(k8sResourceTypes)-1 {
			m.resourceCursor++
		}
	case "enter":
		m.selectedType = k8sResourceTypes[m.resourceCursor]
		m.view = k8sViewList
		m.loading = true
		m.resources = nil
		m.statusMsg = ""
		m.search = ""
		m.listCursor = 0
		m.listScroll = 0
		m.listHScroll = 0
		m.selected = make(map[int]bool)
		return m, tea.Batch(
			k8sLoadResources(m.selectedType.command, m.namespace),
			k8sLoadTop(m.selectedType.command, m.namespace),
			k8sSpinnerTick(), k8sAutoRefreshCmd(), k8sTopRefreshCmd(),
		)
	case "ctrl+n":
		m.nsInputFrom = k8sViewResources
		m.loading = true
		return m, tea.Batch(k8sLoadNamespaces(), k8sSpinnerTick())
	case "esc", "q":
		m.quitting = true
		return m, tea.Quit
	}
	return m, nil
}

func (m k8sModel) handleListKey(key string) (tea.Model, tea.Cmd) {
	if m.loading {
		return m, nil
	}

	// ── search input mode (activated by /) ──
	if m.searching {
		switch key {
		case "esc":
			m.searching = false
			if m.search == "" {
				// nothing typed, just close
			}
			return m, nil
		case "enter":
			m.searching = false
			return m, nil
		case "backspace":
			if len(m.search) > 0 {
				m.search = m.search[:len(m.search)-1]
				m.k8sApplyFilter()
				m.listCursor = 0
				m.listScroll = 0
			}
			return m, nil
		default:
			if len(key) == 1 && key[0] >= 32 && key[0] < 127 {
				m.search += key
				m.k8sApplyFilter()
				m.listCursor = 0
				m.listScroll = 0
			}
			return m, nil
		}
	}

	// ── normal mode ──
	switch key {
	case "esc":
		if m.search != "" {
			m.search = ""
			m.k8sApplyFilter()
			m.listCursor = 0
			m.listScroll = 0
		} else {
			m.view = k8sViewResources
			m.resources = nil
			m.statusMsg = ""
			m.selected = make(map[int]bool)
		}
		return m, nil
	case "/":
		m.searching = true
		m.search = ""
		m.k8sApplyFilter()
		m.listCursor = 0
		m.listScroll = 0
		return m, nil
	case "up":
		if m.listCursor > 0 {
			m.listCursor--
			m.ensureListVisible()
		}
	case "down":
		if m.listCursor < len(m.filtered)-1 {
			m.listCursor++
			m.ensureListVisible()
		}
	case "left":
		if m.listHScroll > 0 {
			m.listHScroll--
		}
	case "right":
		m.listHScroll++
	case "home":
		m.listCursor = 0
		m.listScroll = 0
	case "end":
		if len(m.filtered) > 0 {
			m.listCursor = len(m.filtered) - 1
			m.ensureListVisible()
		}
	case "pgup":
		m.listCursor -= 10
		if m.listCursor < 0 {
			m.listCursor = 0
		}
		m.ensureListVisible()
	case "pgdown":
		m.listCursor += 10
		if m.listCursor >= len(m.filtered) {
			m.listCursor = len(m.filtered) - 1
		}
		if m.listCursor < 0 {
			m.listCursor = 0
		}
		m.ensureListVisible()
	case " ", "tab":
		// toggle multi-select on current item
		if len(m.filtered) > 0 && m.listCursor < len(m.filtered) {
			idx := m.filtered[m.listCursor]
			if m.selected[idx] {
				delete(m.selected, idx)
			} else {
				m.selected[idx] = true
			}
			// move cursor down after toggle
			if m.listCursor < len(m.filtered)-1 {
				m.listCursor++
				m.ensureListVisible()
			}
		}
	case "enter":
		if len(m.filtered) > 0 && m.listCursor < len(m.filtered) && m.filtered[m.listCursor] < len(m.resources) {
			r := m.resources[m.filtered[m.listCursor]]
			m.loading = true
			return m, tea.Batch(k8sDescribe(m.selectedType.command, r.name, m.namespace), k8sSpinnerTick())
		}
	case "ctrl+l":
		if len(m.filtered) > 0 && m.listCursor < len(m.filtered) && m.filtered[m.listCursor] < len(m.resources) {
			r := m.resources[m.filtered[m.listCursor]]
			m.loading = true
			switch m.selectedType.command {
			case "pods":
				return m, tea.Batch(k8sGetLogs(r.name, m.namespace, 500), k8sSpinnerTick())
			case "deployments", "statefulsets", "daemonsets", "replicasets", "jobs":
				return m, tea.Batch(k8sGetLogsByLabel("app="+r.name, m.namespace, 500), k8sSpinnerTick())
			default:
				m.loading = false
			}
		}
	case "ctrl+d":
		// collect targets: selected items or current cursor
		var targets []string
		if len(m.selected) > 0 {
			for idx := range m.selected {
				if idx < len(m.resources) {
					targets = append(targets, m.resources[idx].name)
				}
			}
		} else if len(m.filtered) > 0 && m.listCursor < len(m.filtered) && m.filtered[m.listCursor] < len(m.resources) {
			targets = append(targets, m.resources[m.filtered[m.listCursor]].name)
		}
		if len(targets) > 0 {
			sort.Strings(targets)
			m.deleteTargets = targets
			m.view = k8sViewDelete
		}
		return m, nil
	case "ctrl+a":
		// select/deselect all visible
		if len(m.selected) == len(m.filtered) {
			m.selected = make(map[int]bool)
		} else {
			for _, idx := range m.filtered {
				m.selected[idx] = true
			}
		}
		return m, nil
	case "ctrl+r":
		m.loading = true
		m.selected = make(map[int]bool)
		// clear pod resource data so it reloads fresh
		for i := range m.resources {
			m.resources[i].cpuReq = ""
			m.resources[i].memReq = ""
			m.resources[i].cpuLim = ""
			m.resources[i].memLim = ""
		}
		cmds := []tea.Cmd{
			k8sLoadResources(m.selectedType.command, m.namespace),
			k8sLoadTop(m.selectedType.command, m.namespace),
			k8sSpinnerTick(),
		}
		if m.selectedType.command == "pods" {
			cmds = append(cmds, k8sLoadPodResources(m.namespace))
		}
		return m, tea.Batch(cmds...)
	case "ctrl+n":
		m.nsInputFrom = k8sViewList
		m.loading = true
		return m, tea.Batch(k8sLoadNamespaces(), k8sSpinnerTick())
	}
	return m, nil
}

func (m k8sModel) handleViewerKey(key string) (tea.Model, tea.Cmd) {
	maxScroll := len(m.viewLines) - (m.height - 6)
	if maxScroll < 0 {
		maxScroll = 0
	}

	// if searching, handle search input
	if m.logSearching {
		switch key {
		case "esc":
			m.logSearching = false
			return m, nil
		case "enter":
			m.logSearching = false
			m.k8sApplyLogSearch()
			if len(m.logMatches) > 0 {
				m.logMatchIdx = 0
				m.viewScroll = m.logMatches[0]
			}
			return m, nil
		case "backspace":
			if len(m.logSearch) > 0 {
				m.logSearch = m.logSearch[:len(m.logSearch)-1]
			}
			return m, nil
		default:
			if len(key) == 1 && key[0] >= 32 && key[0] < 127 {
				m.logSearch += key
			}
			return m, nil
		}
	}

	switch key {
	case "esc":
		if m.logSearch != "" {
			m.logSearch = ""
			m.logMatches = nil
			return m, nil
		}
		m.view = k8sViewList
		m.viewContent = ""
		m.viewLines = nil
		m.logSearch = ""
		m.logMatches = nil
		return m, k8sAutoRefreshCmd()
	case "q":
		m.view = k8sViewList
		m.viewContent = ""
		m.viewLines = nil
		m.logSearch = ""
		m.logMatches = nil
		return m, k8sAutoRefreshCmd()
	case "/":
		m.logSearching = true
		m.logSearch = ""
		m.logMatches = nil
		return m, nil
	case "ctrl+n":
		// next match
		if len(m.logMatches) > 0 {
			m.logMatchIdx = (m.logMatchIdx + 1) % len(m.logMatches)
			m.viewScroll = m.logMatches[m.logMatchIdx]
		}
		return m, nil
	case "ctrl+p":
		// prev match
		if len(m.logMatches) > 0 {
			m.logMatchIdx--
			if m.logMatchIdx < 0 {
				m.logMatchIdx = len(m.logMatches) - 1
			}
			m.viewScroll = m.logMatches[m.logMatchIdx]
		}
		return m, nil
	case "up", "k":
		if m.viewScroll > 0 {
			m.viewScroll--
		}
	case "down", "j":
		if m.viewScroll < maxScroll {
			m.viewScroll++
		}
	case "pgup":
		m.viewScroll -= 20
		if m.viewScroll < 0 {
			m.viewScroll = 0
		}
	case "pgdown":
		m.viewScroll += 20
		if m.viewScroll > maxScroll {
			m.viewScroll = maxScroll
		}
	case "home", "g":
		m.viewScroll = 0
		m.viewHScroll = 0
	case "end", "G":
		m.viewScroll = maxScroll
	case "left":
		if m.viewHScroll > 0 {
			m.viewHScroll -= 10
			if m.viewHScroll < 0 {
				m.viewHScroll = 0
			}
		}
	case "right":
		m.viewHScroll += 10
	case "shift+left":
		m.viewHScroll = 0
	case "shift+right":
		m.viewHScroll += 40
	}
	return m, nil
}

func (m *k8sModel) k8sApplyLogSearch() {
	m.logMatches = nil
	m.logMatchSet = nil
	if m.logSearch == "" {
		return
	}
	needle := strings.ToLower(m.logSearch)
	m.logMatchSet = make(map[int]bool)
	for i, line := range m.viewLines {
		if strings.Contains(strings.ToLower(line), needle) {
			m.logMatches = append(m.logMatches, i)
			m.logMatchSet[i] = true
		}
	}
}

func (m k8sModel) handleDeleteKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "y", "Y":
		m.loading = true
		m.view = k8sViewList
		targets := m.deleteTargets
		m.deleteTargets = nil
		m.selected = make(map[int]bool)
		return m, tea.Batch(k8sDeleteResource(m.selectedType.command, targets, m.namespace), k8sSpinnerTick())
	case "n", "N", "esc":
		m.view = k8sViewList
		m.deleteTargets = nil
	}
	return m, nil
}

func (m k8sModel) handleNamespacesKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		if m.nsSearch != "" {
			m.nsSearch = ""
			m.k8sApplyNsFilter()
			m.nsCursor = 0
			m.nsScroll = 0
		} else if m.selectedType.command != "" {
			m.view = k8sViewList
			return m, k8sAutoRefreshCmd()
		} else {
			m.view = k8sViewResources
		}
		return m, nil
	case "up", "k":
		if m.nsCursor > 0 {
			m.nsCursor--
			m.ensureNsVisible()
		}
	case "down", "j":
		if m.nsCursor < len(m.nsFiltered)-1 {
			m.nsCursor++
			m.ensureNsVisible()
		}
	case "enter":
		if len(m.nsFiltered) > 0 {
			m.namespace = m.namespaces[m.nsFiltered[m.nsCursor]]
			m.selected = make(map[int]bool)
			// set as default namespace in kubeconfig
			exec.Command("kubectl", "config", "set-context", "--current", "--namespace="+m.namespace).Run()
			if m.selectedType.command != "" {
				m.view = k8sViewList
				m.loading = true
				return m, tea.Batch(
					k8sLoadResources(m.selectedType.command, m.namespace),
					k8sLoadTop(m.selectedType.command, m.namespace),
					k8sSpinnerTick(), k8sAutoRefreshCmd(), k8sTopRefreshCmd(),
				)
			}
			m.view = k8sViewResources
		}
		return m, nil
	case "backspace":
		if len(m.nsSearch) > 0 {
			m.nsSearch = m.nsSearch[:len(m.nsSearch)-1]
			m.k8sApplyNsFilter()
			m.nsCursor = 0
			m.nsScroll = 0
		}
	default:
		if len(key) == 1 && key[0] >= 32 && key[0] < 127 {
			m.nsSearch += key
			m.k8sApplyNsFilter()
			m.nsCursor = 0
			m.nsScroll = 0
		}
	}
	return m, nil
}

func (m k8sModel) handleNsInputKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		m.view = m.nsInputFrom
		m.nsInput = ""
		return m, nil
	case "enter":
		ns := strings.TrimSpace(m.nsInput)
		if ns != "" {
			m.namespace = ns
			exec.Command("kubectl", "config", "set-context", "--current", "--namespace="+ns).Run()
		}
		m.nsInput = ""
		if m.nsInputFrom == k8sViewList && m.selectedType.command != "" {
			m.view = k8sViewList
			m.loading = true
			return m, tea.Batch(
				k8sLoadResources(m.selectedType.command, m.namespace),
				k8sLoadTop(m.selectedType.command, m.namespace),
				k8sSpinnerTick(), k8sAutoRefreshCmd(), k8sTopRefreshCmd(),
			)
		}
		m.view = m.nsInputFrom
		return m, nil
	case "backspace":
		if len(m.nsInput) > 0 {
			m.nsInput = m.nsInput[:len(m.nsInput)-1]
		}
	default:
		if len(key) == 1 && key[0] >= 32 && key[0] < 127 {
			m.nsInput += key
		}
	}
	return m, nil
}

// ── View ───────────────────────────────────────────────

// k8sViewResult holds content lines and a footer that gets pinned to the bottom
type k8sViewResult struct {
	content []string
	footer  []string
}

func (m k8sModel) View() string {
	if m.quitting || m.width == 0 {
		return ""
	}
	iw := m.width - 2

	bar := k8sBarSt.Render(strings.Repeat("─", iw))

	// header
	ctxShort := m.context
	if idx := strings.LastIndex(ctxShort, "/"); idx >= 0 {
		ctxShort = ctxShort[idx+1:]
	}
	hdr := " " + k8sTitleSt.Render("⎈ ksw k8s") +
		"  " + k8sDimSt.Render(ctxShort) +
		"  " + k8sLabelSt.Render("ns:") + k8sOkSt.Render(m.namespace)
	if !m.lastRefresh.IsZero() {
		ago := time.Since(m.lastRefresh).Truncate(time.Second)
		hdr += "  " + k8sDimSt.Render(fmt.Sprintf("↻ %s", ago))
	}

	var vr k8sViewResult
	switch m.view {
	case k8sViewResources:
		vr = m.viewResources(iw)
	case k8sViewList:
		vr = m.viewList(iw)
	case k8sViewDescribe:
		vr = m.viewDescribeView(iw)
	case k8sViewLogs:
		vr = m.viewLogsView(iw)
	case k8sViewDelete:
		vr = m.viewDeleteConfirm()
	case k8sViewNamespaces:
		vr = m.viewNamespacePicker(iw)
	case k8sViewNsInput:
		vr = m.viewNsInput(iw)
	}

	// layout: header(1) + bar(1) + content + padding + footer + bar(1)
	fixedLines := 3 + len(vr.footer) // header + top bar + bottom bar + footer
	contentSpace := m.height - fixedLines
	if contentSpace < 3 {
		contentSpace = 3
	}

	var b strings.Builder
	b.WriteString(hdr + "\n")
	b.WriteString(" " + bar + "\n")

	// write content lines, pad to fill
	for i := 0; i < contentSpace; i++ {
		if i < len(vr.content) {
			b.WriteString(vr.content[i] + "\n")
		} else {
			b.WriteString("\n")
		}
	}

	// footer pinned to bottom
	for _, fl := range vr.footer {
		b.WriteString(fl + "\n")
	}
	b.WriteString(" " + bar)
	return b.String()
}

func (m k8sModel) viewResources(iw int) k8sViewResult {
	var c []string
	c = append(c, "")
	c = append(c, " "+k8sLabelSt.Render("Resources")+"  "+k8sDimSt.Render("select a resource type"))
	c = append(c, "")
	for i, rt := range k8sResourceTypes {
		tag := k8sDimSt.Render(" ("+rt.short+")")
		if i == m.resourceCursor {
			c = append(c, "  "+k8sSelSt.Render("❯ "+rt.name)+tag)
		} else {
			c = append(c, "    "+k8sNormalSt.Render(rt.name)+tag)
		}
	}

	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + k8sDimSt.Render(" ↑↓ navigate · enter select · ^n namespace · q quit"),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── List view with dynamic table ───────────────────────

func (m k8sModel) viewList(iw int) k8sViewResult {
	var c []string

	// title with selection count
	titleExtra := k8sDimSt.Render(fmt.Sprintf(" (%d)", len(m.filtered)))
	if len(m.selected) > 0 {
		titleExtra += "  " + k8sWarnSt.Render(fmt.Sprintf("✓ %d selected", len(m.selected)))
	}
	c = append(c, " "+k8sLabelSt.Render(m.selectedType.name)+titleExtra)

	// search
	if m.searching {
		c = append(c, " "+searchActiveStyle.Render(" / "+m.search+"█")+"  "+k8sDimSt.Render("enter confirm · esc cancel"))
	} else if m.search != "" {
		c = append(c, " "+searchActiveStyle.Render(" / "+m.search)+"  "+k8sDimSt.Render("esc clear"))
	}

	// status msg
	if m.statusMsg != "" {
		c = append(c, " "+k8sWarnSt.Render(" "+m.statusMsg))
	}

	// footer (always built)
	cnt := counterStyle.Render(fmt.Sprintf(" %d/%d", len(m.filtered), len(m.resources)))
	var help string
	if m.selectedType.command == "pods" {
		if m.width >= 120 {
			help = "  ↑↓ navigate · ←→ scroll · space select · ^a all · enter desc · ^l logs · / search · ^r refresh · ^n ns · ^d delete · esc"
		} else {
			help = "  ↑↓ · ←→ · space sel · ^a · enter · ^l · / · ^r · ^n · ^d · esc"
		}
	} else {
		if m.width >= 110 {
			help = "  ↑↓ navigate · ←→ scroll · space select · ^a all · enter desc · / search · ^r refresh · ^n ns · ^d delete · esc"
		} else {
			help = "  ↑↓ · ←→ · space sel · ^a · enter · / · ^r · ^n · ^d · esc"
		}
	}
	hscrollInfo := ""
	if m.listHScroll > 0 {
		hscrollInfo = "  " + k8sDimSt.Render(fmt.Sprintf("col+%d", m.listHScroll))
	}
	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + cnt + k8sDimSt.Render(help) + hscrollInfo,
	}

	if m.loading {
		sp := k8sSpinner[m.spinnerFrame]
		c = append(c, "")
		c = append(c, " "+k8sOkSt.Render(sp)+" "+k8sDimSt.Render("Loading..."))
		return k8sViewResult{content: c, footer: footer}
	}

	if len(m.filtered) == 0 {
		c = append(c, "")
		if m.statusMsg != "" {
			c = append(c, " "+k8sDimSt.Render(" esc back · r retry"))
		} else {
			c = append(c, " "+k8sDimSt.Render(" No resources found"))
		}
		return k8sViewResult{content: c, footer: footer}
	}

	// build ALL columns with natural widths
	allCols := m.k8sBuildAllColumns()
	allWidths := make([]int, len(allCols))
	gap := 2
	for ci, col := range allCols {
		w := utf8.RuneCountInString(col.header)
		for _, v := range col.values {
			if vw := utf8.RuneCountInString(v); vw > w {
				w = vw
			}
		}
		allWidths[ci] = w + gap
	}

	// horizontal scroll: skip columns after NAME
	hSkip := m.listHScroll
	if hSkip < 0 {
		hSkip = 0
	}
	maxSkip := len(allCols) - 2 // keep at least NAME + 1 column
	if maxSkip < 0 {
		maxSkip = 0
	}
	if hSkip > maxSkip {
		hSkip = maxSkip
	}

	// NAME always visible, then skip hSkip columns, then fit remaining
	ptrW := 3 // status dot + cursor + space
	availForCols := iw - ptrW
	visCols := []k8sCol{allCols[0]}
	visWidths := []int{allWidths[0]}
	usedW := allWidths[0]

	remaining := allCols[1:]
	remWidths := allWidths[1:]
	if hSkip < len(remaining) {
		remaining = remaining[hSkip:]
		remWidths = remWidths[hSkip:]
	} else {
		remaining = nil
		remWidths = nil
	}
	moreRight := false
	for ci, col := range remaining {
		cw := remWidths[ci]
		if usedW+cw <= availForCols {
			visCols = append(visCols, col)
			visWidths = append(visWidths, cw)
			usedW += cw
		} else {
			moreRight = true
			break
		}
	}
	// give leftover to NAME
	if usedW < availForCols {
		visWidths[0] += availForCols - usedW
	}

	// table header
	var hdr strings.Builder
	hdr.WriteString("   ") // align with status dot + cursor + space
	for ci, col := range visCols {
		hdr.WriteString(k8sHeaderSt.Render(k8sPad(col.header, visWidths[ci])))
	}
	scrollHint := ""
	if hSkip > 0 {
		scrollHint = k8sDimSt.Render(" ◀")
	}
	if moreRight {
		scrollHint += k8sDimSt.Render("▶")
	}
	c = append(c, hdr.String()+scrollHint)

	// rows
	maxVis := m.maxListVisible()
	start := m.listScroll
	end := start + maxVis
	if end > len(m.filtered) {
		end = len(m.filtered)
	}

	if start > 0 {
		c = append(c, " "+k8sDimSt.Render(fmt.Sprintf("  ▲ %d more", start)))
	}

	for i := start; i < end; i++ {
		idx := m.filtered[i]
		if idx >= len(m.resources) {
			continue
		}
		r := m.resources[idx]
		isCursor := i == m.listCursor
		isSelected := m.selected[idx]
		isAlt := (i-start)%2 == 1

		// status dot — colored circle based on resource status
		cls := k8sClassifyStatus(r.status)
		// for resources without status (deployments, etc.), infer from READY field
		if r.status == "" && r.ready != "" {
			parts := strings.SplitN(r.ready, "/", 2)
			if len(parts) == 2 {
				if parts[0] == parts[1] && parts[0] != "0" {
					cls = k8sClassOk
				} else if parts[0] == "0" {
					cls = k8sClassErr
				} else {
					cls = k8sClassWarn
				}
			}
		}
		var dot string
		switch cls {
		case k8sClassOk:
			dot = k8sOkSt.Render("●")
		case k8sClassWarn:
			dot = k8sWarnSt.Render("●")
		case k8sClassErr:
			dot = k8sErrSt.Render("●")
		case k8sClassDim:
			dot = k8sDimSt.Render("○")
		default:
			dot = k8sDimSt.Render("○")
		}
		// override dot with selection marker
		if isSelected {
			dot = k8sOkSt.Render("◉")
		}

		// cursor indicator
		cur := " "
		if isCursor {
			cur = k8sSelSt.Render("❯")
		}

		// build styled cells
		var row strings.Builder
		row.WriteString(dot + cur + " ")
		for ci, col := range visCols {
			val := ""
			if i < len(col.values) {
				val = col.values[i]
			}
			padded := k8sPad(val, visWidths[ci])
			row.WriteString(k8sStyleCell(col.header, val, padded, visWidths[ci], isCursor, isAlt))
		}
		c = append(c, row.String())
	}

	if end < len(m.filtered) {
		c = append(c, " "+k8sDimSt.Render(fmt.Sprintf("  ▼ %d more", len(m.filtered)-end)))
	}

	return k8sViewResult{content: c, footer: footer}
}

// ── Describe view with syntax highlighting ─────────────

func (m k8sModel) viewDescribeView(iw int) k8sViewResult {
	var c []string

	// search bar
	if m.logSearching {
		c = append(c, " "+searchActiveStyle.Render(" / "+m.logSearch+"█")+"  "+k8sDimSt.Render("enter to search · esc cancel"))
	} else if m.logSearch != "" {
		var matchInfo string
		if len(m.logMatches) > 0 {
			matchInfo = k8sMatchLine.Render(fmt.Sprintf(" %d/%d matches", m.logMatchIdx+1, len(m.logMatches)))
		} else {
			matchInfo = k8sErrSt.Render(" no matches")
		}
		c = append(c, " "+k8sDimSt.Render(" /")+k8sMatchHl.Render(m.logSearch)+matchInfo+
			"  "+k8sDimSt.Render("^n next · ^p prev · esc clear"))
	}

	if m.loading {
		sp := k8sSpinner[m.spinnerFrame]
		c = append(c, "")
		c = append(c, " "+k8sOkSt.Render(sp)+" "+k8sDimSt.Render("Loading..."))
		return k8sViewResult{content: c, footer: []string{
			" " + k8sBarSt.Render(strings.Repeat("─", iw)),
			" " + k8sDimSt.Render(" esc back · / search · ↑↓ scroll · g/G top/bottom"),
		}}
	}

	maxLines := m.height - 7
	if m.logSearch != "" || m.logSearching {
		maxLines--
	}
	if maxLines < 5 {
		maxLines = 5
	}
	end := m.viewScroll + maxLines
	if end > len(m.viewLines) {
		end = len(m.viewLines)
	}

	currentMatchLine := -1
	if len(m.logMatches) > 0 && m.logMatchIdx < len(m.logMatches) {
		currentMatchLine = m.logMatches[m.logMatchIdx]
	}

	for i := m.viewScroll; i < end; i++ {
		line := m.viewLines[i]

		// apply horizontal scroll
		visLine := line
		if m.viewHScroll > 0 {
			runes := []rune(visLine)
			if m.viewHScroll < len(runes) {
				visLine = string(runes[m.viewHScroll:])
			} else {
				visLine = ""
			}
		}
		runes := []rune(visLine)
		if len(runes) > iw-4 {
			visLine = string(runes[:iw-5])
			visLine += k8sDimSt.Render("→")
		}

		if m.logSearch != "" && m.logMatchSet[i] {
			highlighted := k8sHighlightMatch(visLine, m.logSearch)
			if i == currentMatchLine {
				c = append(c, " "+k8sMatchLine.Render("▸")+" "+highlighted)
			} else {
				c = append(c, " "+k8sMatchLine.Render("│")+" "+highlighted)
			}
		} else {
			c = append(c, "  "+k8sHighlightDescribe(visLine))
		}
	}

	total := len(m.viewLines)
	pct := 0
	if total > 0 {
		pct = (m.viewScroll + maxLines) * 100 / total
		if pct > 100 {
			pct = 100
		}
	}
	footerHelp := " describe"
	if m.logSearch == "" {
		footerHelp += " · / search"
	}
	footerHelp += fmt.Sprintf(" · line %d-%d/%d (%d%%)", m.viewScroll+1, end, total, pct)
	if m.viewHScroll > 0 {
		footerHelp += fmt.Sprintf(" · col %d", m.viewHScroll+1)
	}
	footerHelp += " · ↑↓ ←→ · g/G · esc"
	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + k8sDimSt.Render(footerHelp),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── Logs view with syntax highlighting ─────────────────

func (m k8sModel) viewLogsView(iw int) k8sViewResult {
	var c []string

	// search bar
	if m.logSearching {
		c = append(c, " "+searchActiveStyle.Render(" / "+m.logSearch+"█")+"  "+k8sDimSt.Render("enter to search · esc cancel"))
	} else if m.logSearch != "" {
		var matchInfo string
		if len(m.logMatches) > 0 {
			matchInfo = k8sMatchLine.Render(fmt.Sprintf(" %d/%d matches", m.logMatchIdx+1, len(m.logMatches)))
		} else {
			matchInfo = k8sErrSt.Render(" no matches")
		}
		c = append(c, " "+k8sDimSt.Render(" /")+k8sMatchHl.Render(m.logSearch)+matchInfo+
			"  "+k8sDimSt.Render("^n next · ^p prev · esc clear"))
	}

	if m.loading {
		sp := k8sSpinner[m.spinnerFrame]
		c = append(c, "")
		c = append(c, " "+k8sOkSt.Render(sp)+" "+k8sDimSt.Render("Loading..."))
		return k8sViewResult{content: c, footer: []string{
			" " + k8sBarSt.Render(strings.Repeat("─", iw)),
			" " + k8sDimSt.Render(" esc back · / search · ↑↓ scroll · g/G top/bottom"),
		}}
	}

	maxVisLines := m.height - 7
	if m.logSearch != "" || m.logSearching {
		maxVisLines--
	}
	if maxVisLines < 5 {
		maxVisLines = 5
	}

	// match lookup
	currentMatchLine := -1
	if len(m.logMatches) > 0 && m.logMatchIdx < len(m.logMatches) {
		currentMatchLine = m.logMatches[m.logMatchIdx]
	}

	lineNumW := len(fmt.Sprintf("%d", len(m.viewLines)))
	contentW := iw - lineNumW - 3
	if contentW < 20 {
		contentW = 20
	}

	// render lines with wrap, starting from viewScroll
	rendered := 0
	end := m.viewScroll
	for i := m.viewScroll; i < len(m.viewLines) && rendered < maxVisLines; i++ {
		line := m.viewLines[i]
		lineNum := k8sDimSt.Render(fmt.Sprintf("%*d", lineNumW, i+1))
		isMatch := m.logSearch != "" && m.logMatchSet[i]
		isCurrent := i == currentMatchLine

		// gutter marker
		gutter := k8sDimSt.Render("│")
		if isMatch && isCurrent {
			gutter = k8sMatchLine.Render("▸")
		} else if isMatch {
			gutter = k8sMatchLine.Render("│")
		}

		// wrap the line into chunks of contentW
		runes := []rune(line)
		if len(runes) == 0 {
			c = append(c, " "+gutter+lineNum+" ")
			rendered++
		} else {
			first := true
			for len(runes) > 0 && rendered < maxVisLines {
				chunkSize := contentW
				if chunkSize > len(runes) {
					chunkSize = len(runes)
				}
				chunk := string(runes[:chunkSize])
				runes = runes[chunkSize:]

				var styledChunk string
				if isMatch {
					styledChunk = k8sHighlightMatch(chunk, m.logSearch)
				} else {
					styledChunk = k8sHighlightLog(chunk)
				}

				if first {
					c = append(c, " "+gutter+lineNum+" "+styledChunk)
					first = false
				} else {
					// continuation line — dim gutter, no line number
					pad := strings.Repeat(" ", lineNumW)
					c = append(c, " "+k8sDimSt.Render("·")+pad+" "+styledChunk)
				}
				rendered++
			}
		}
		end = i + 1
	}

	total := len(m.viewLines)
	pct := 0
	if total > 0 {
		pct = end * 100 / total
		if pct > 100 {
			pct = 100
		}
	}
	footerHelp := " logs"
	if m.logSearch == "" {
		footerHelp += " · / search"
	}
	footerHelp += fmt.Sprintf(" · line %d-%d/%d (%d%%) · ↑↓ pgup/pgdn · g/G · esc", m.viewScroll+1, end, total, pct)
	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + k8sDimSt.Render(footerHelp),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── Delete confirmation ────────────────────────────────

func (m k8sModel) viewDeleteConfirm() k8sViewResult {
	var c []string
	c = append(c, "")
	if len(m.deleteTargets) == 1 {
		c = append(c, " "+k8sErrSt.Render(fmt.Sprintf(" ⚠  Delete %s '%s' in ns '%s'?", m.selectedType.name, m.deleteTargets[0], m.namespace)))
	} else {
		c = append(c, " "+k8sErrSt.Render(fmt.Sprintf(" ⚠  Delete %d %s in ns '%s'?", len(m.deleteTargets), m.selectedType.name, m.namespace)))
		c = append(c, "")
		for _, t := range m.deleteTargets {
			c = append(c, "   "+k8sWarnSt.Render("• "+t))
		}
	}
	c = append(c, "")
	c = append(c, " "+k8sDimSt.Render(" This action cannot be undone."))

	footer := []string{
		"",
		" " + k8sErrSt.Render(" → y") + " " + k8sDimSt.Render("confirm") + "    " + k8sDimSt.Render("n/esc cancel"),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── Namespace picker ───────────────────────────────────

func (m k8sModel) viewNamespacePicker(iw int) k8sViewResult {
	var c []string
	c = append(c, "")
	c = append(c, " "+k8sLabelSt.Render(" Namespaces")+"  "+k8sDimSt.Render("select a namespace"))
	c = append(c, "")
	if m.nsSearch != "" {
		c = append(c, " "+searchActiveStyle.Render(" ❯ "+m.nsSearch+"█"))
	} else {
		c = append(c, " "+searchPlaceholderStyle.Render(" ❯ type to search..."))
	}
	c = append(c, " "+k8sBarSt.Render(strings.Repeat("─", iw)))

	if m.loading {
		sp := k8sSpinner[m.spinnerFrame]
		c = append(c, "")
		c = append(c, " "+k8sOkSt.Render(sp)+" "+k8sDimSt.Render("Loading namespaces..."))
		return k8sViewResult{content: c, footer: []string{}}
	}
	if len(m.nsFiltered) == 0 {
		c = append(c, "")
		c = append(c, " "+k8sDimSt.Render(" No namespaces found"))
	} else {
		maxVis := m.height - 10
		if maxVis < 5 {
			maxVis = 5
		}
		start := m.nsScroll
		end := start + maxVis
		if end > len(m.nsFiltered) {
			end = len(m.nsFiltered)
		}
		if start > 0 {
			c = append(c, " "+k8sDimSt.Render(fmt.Sprintf("  ▲ %d more", start)))
		}
		for i := start; i < end; i++ {
			ns := m.namespaces[m.nsFiltered[i]]
			cur := ""
			if ns == m.namespace {
				cur = " " + k8sOkSt.Render("●")
			}
			if i == m.nsCursor {
				c = append(c, "  "+k8sSelSt.Render("❯ "+ns)+cur)
			} else {
				c = append(c, "    "+k8sNormalSt.Render(ns)+cur)
			}
		}
		if end < len(m.nsFiltered) {
			c = append(c, " "+k8sDimSt.Render(fmt.Sprintf("  ▼ %d more", len(m.nsFiltered)-end)))
		}
	}

	cnt := counterStyle.Render(fmt.Sprintf(" %d/%d", len(m.nsFiltered), len(m.namespaces)))
	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + cnt + k8sDimSt.Render("  ↑↓ · enter select · esc back"),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── Namespace input ────────────────────────────────────

func (m k8sModel) viewNsInput(iw int) k8sViewResult {
	var c []string
	c = append(c, "")
	c = append(c, " "+k8sLabelSt.Render(" Change namespace"))
	c = append(c, "")
	c = append(c, " "+k8sDimSt.Render(" Current: ")+k8sOkSt.Render(m.namespace))
	c = append(c, "")
	c = append(c, " "+searchActiveStyle.Render(" namespace ❯ "+m.nsInput+"█"))
	c = append(c, "")
	c = append(c, " "+k8sDimSt.Render(" Type the namespace name and press enter."))

	footer := []string{
		" " + k8sBarSt.Render(strings.Repeat("─", iw)),
		" " + k8sDimSt.Render(" enter confirm · esc cancel"),
	}
	return k8sViewResult{content: c, footer: footer}
}

// ── Entry point ────────────────────────────────────────

func handleK8sTUI(args []string) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		fmt.Fprintf(os.Stderr, "%s kubectl not found in PATH\n", warnStyle.Render("✗"))
		os.Exit(1)
	}

	// parse args first (no I/O)
	directResource := ""
	nsOverride := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-n", "--namespace":
			if i+1 < len(args) {
				nsOverride = args[i+1]
				i++
			}
		default:
			for _, rt := range k8sResourceTypes {
				if strings.EqualFold(args[i], rt.command) || strings.EqualFold(args[i], rt.short) || strings.EqualFold(args[i], rt.name) {
					directResource = rt.command
					break
				}
			}
		}
	}
	if directResource == "" {
		directResource = "pods"
	}

	// launch context + namespace in parallel
	type ctxResult struct{ ctx, ns string }
	ctxCh := make(chan ctxResult, 1)
	go func() {
		var wgCtx, wgNs string
		done := make(chan struct{}, 2)
		go func() {
			out, err := exec.Command("kubectl", "config", "current-context").Output()
			if err == nil {
				wgCtx = strings.TrimSpace(string(out))
			}
			done <- struct{}{}
		}()
		go func() {
			if nsOverride != "" {
				wgNs = nsOverride
				done <- struct{}{}
				return
			}
			out, _ := exec.Command("kubectl", "config", "view", "--minify", "--output", "jsonpath={.contexts[0].context.namespace}").Output()
			wgNs = strings.TrimSpace(string(out))
			if wgNs == "" {
				wgNs = "default"
			}
			done <- struct{}{}
		}()
		<-done
		<-done
		ctxCh <- ctxResult{wgCtx, wgNs}
	}()

	// resolve the selected resource type
	var selectedType k8sResourceType
	for _, rt := range k8sResourceTypes {
		if rt.command == directResource {
			selectedType = rt
			break
		}
	}

	// wait for context/namespace (these are fast local calls)
	cr := <-ctxCh
	if cr.ctx == "" {
		fmt.Fprintf(os.Stderr, "%s No active Kubernetes context. Use 'ksw' to select one.\n", warnStyle.Render("✗"))
		os.Exit(1)
	}

	mdl := k8sModel{
		view:         k8sViewList,
		context:      cr.ctx,
		namespace:    cr.ns,
		selected:     make(map[int]bool),
		selectedType: selectedType,
		loading:      true,
	}

	p := tea.NewProgram(&mdl, tea.WithAltScreen())

	// fire ALL kubectl calls in parallel immediately
	go func() {
		p.Send(k8sSpinnerTickMsg{})

		ns := mdl.namespace
		rt := mdl.selectedType.command

		// 1. get resources
		go func() {
			rArgs := []string{"get", rt, "-o", "wide", "--no-headers=false", "--request-timeout=8s"}
			if rt != "nodes" && rt != "namespaces" {
				rArgs = append(rArgs, "-n", ns)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, "kubectl", rArgs...).CombinedOutput()
			if err != nil {
				p.Send(k8sResourcesLoadedMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))})
			} else {
				res, hdr := k8sParseResources(string(out))
				p.Send(k8sResourcesLoadedMsg{resources: res, headers: hdr})
			}
		}()

		// 2. top metrics (parallel)
		go func() {
			topType := ""
			if rt == "pods" {
				topType = "pods"
			} else if rt == "nodes" {
				topType = "nodes"
			}
			if topType == "" {
				return
			}
			tArgs := []string{"top", topType, "--no-headers", "--request-timeout=4s"}
			if topType == "pods" {
				tArgs = append(tArgs, "-n", ns)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			tOut, tErr := exec.CommandContext(ctx, "kubectl", tArgs...).CombinedOutput()
			if tErr != nil {
				return
			}
			met := make(map[string][2]string)
			for _, line := range strings.Split(strings.TrimSpace(string(tOut)), "\n") {
				f := strings.Fields(line)
				if len(f) >= 3 {
					met[f[0]] = [2]string{f[1], f[2]}
				}
			}
			p.Send(k8sTopLoadedMsg{metrics: met})
		}()

		// 3. pod resources (requests/limits) — parallel, only for pods
		if rt == "pods" {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				defer cancel()
				out, err := exec.CommandContext(ctx, "kubectl", "get", "pods", "-n", ns, "--request-timeout=6s", "-o",
					`jsonpath={range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.resources.requests.cpu}{","}{.resources.requests.memory}{","}{.resources.limits.cpu}{","}{.resources.limits.memory}{" "}{end}{"\n"}{end}`,
				).Output()
				if err != nil {
					return
				}
				m := make(map[string][4]string)
				for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
					parts := strings.SplitN(line, "\t", 2)
					if len(parts) != 2 {
						continue
					}
					name := parts[0]
					first := strings.Fields(parts[1])
					if len(first) == 0 {
						continue
					}
					fields := strings.Split(first[0], ",")
					if len(fields) == 4 {
						m[name] = [4]string{fields[0], fields[1], fields[2], fields[3]}
					}
				}
				p.Send(k8sPodResourcesLoadedMsg{resources: m})
			}()
		}

		// schedule auto-refresh and top refresh
		time.Sleep(30 * time.Second)
		p.Send(k8sAutoRefreshMsg{})
	}()
	go func() {
		time.Sleep(2 * time.Minute)
		p.Send(k8sTopRefreshMsg{})
	}()

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
