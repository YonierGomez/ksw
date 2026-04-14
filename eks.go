package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// ── Estructuras de datos EKS ──────────────────────────

// awsProfile representa un perfil leído de ~/.aws/config
type awsProfile struct {
	Name   string // nombre del perfil (sin el prefijo "profile ")
	Region string // región configurada, o fallback
}

// eksCluster representa un clúster descubierto pendiente de procesar
type eksCluster struct {
	Name    string // nombre del clúster EKS
	Profile string // perfil AWS usado para descubrirlo
	Region  string // región donde se encontró
}

// syncResult acumula los contadores del resumen final
type syncResult struct {
	Added   int // clústeres nuevos agregados al kubeconfig
	Skipped int // clústeres omitidos por ya existir
	Failed  int // clústeres donde update-kubeconfig falló
}

// ── Parseo de perfiles AWS ─────────────────────────────

// parseAWSProfiles lee el archivo de configuración AWS y retorna los perfiles con su región.
// Si configPath está vacío, usa ~/.aws/config por defecto.
func parseAWSProfiles(configPath string) ([]awsProfile, error) {
	if configPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cannot determine home directory: %w", err)
		}
		configPath = filepath.Join(home, ".aws", "config")
	}

	f, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read AWS config file %s: %w", configPath, err)
	}
	defer f.Close()

	var profiles []awsProfile
	var current *awsProfile
	defaultRegion := ""

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			// Save previous profile if any
			if current != nil {
				profiles = append(profiles, *current)
			}

			section := line[1 : len(line)-1]
			section = strings.TrimSpace(section)

			if section == "default" {
				current = &awsProfile{Name: "default"}
			} else if strings.HasPrefix(section, "profile ") {
				name := strings.TrimSpace(strings.TrimPrefix(section, "profile "))
				if name != "" {
					current = &awsProfile{Name: name}
				} else {
					current = nil
				}
			} else {
				// Unknown section type, skip
				current = nil
			}
			continue
		}

		// Key-value pair
		if current != nil && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			if key == "region" && value != "" {
				current.Region = value
				if current.Name == "default" {
					defaultRegion = value
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading AWS config file: %w", err)
	}

	// Save last profile
	if current != nil {
		profiles = append(profiles, *current)
	}

	// Apply fallback region for profiles without one
	fallback := getDefaultRegion()
	if defaultRegion != "" {
		fallback = defaultRegion
	}
	for i := range profiles {
		if profiles[i].Region == "" {
			profiles[i].Region = fallback
		}
	}

	return profiles, nil
}

// getDefaultRegion obtiene la región por defecto de AWS CLI.
// Si falla, retorna "us-east-1" como fallback.
func getDefaultRegion() string {
	out, err := exec.Command("aws", "configure", "get", "region").Output()
	if err != nil {
		return "us-east-1"
	}
	region := strings.TrimSpace(string(out))
	if region == "" {
		return "us-east-1"
	}
	return region
}

// filterProfiles filtra los perfiles según el flag --profile.
// Si profileFilter está vacío, retorna todos los perfiles.
// Si tiene valor, retorna solo el perfil que coincida o error si no existe.
func filterProfiles(profiles []awsProfile, profileFilter string) ([]awsProfile, error) {
	if profileFilter == "" {
		return profiles, nil
	}
	for _, p := range profiles {
		if p.Name == profileFilter {
			return []awsProfile{p}, nil
		}
	}
	return nil, fmt.Errorf("profile '%s' not found in AWS config", profileFilter)
}

// ── Validación de dependencias ──────────────────────────

// checkAWSCLI verifica que "aws" está en el PATH del sistema.
// Retorna error con instrucciones de instalación si no está disponible.
func checkAWSCLI() error {
	_, err := exec.LookPath("aws")
	if err != nil {
		return fmt.Errorf("AWS CLI not found in PATH. Install it from https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
	}
	return nil
}

// ── Descubrimiento de clústeres EKS ─────────────────────

// parseListClustersJSON parsea el JSON de respuesta de "aws eks list-clusters".
// Es una función pura testeable independientemente de listEKSClusters.
// El formato esperado es: {"clusters": ["cluster1", "cluster2"]}
func parseListClustersJSON(data []byte) ([]string, error) {
	var result struct {
		Clusters []string `json:"clusters"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse list-clusters JSON: %w", err)
	}
	return result.Clusters, nil
}

// listEKSClusters ejecuta "aws eks list-clusters" para un perfil/región
// y retorna la lista de nombres de clústeres descubiertos.
// Maneja errores de credenciales y red sin interrumpir la ejecución.
func listEKSClusters(profile, region string) ([]string, error) {
	cmd := exec.Command("aws", "eks", "list-clusters",
		"--profile", profile,
		"--region", region,
		"--output", "json",
	)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			stderr := strings.TrimSpace(string(exitErr.Stderr))
			return nil, fmt.Errorf("%s", stderr)
		}
		return nil, fmt.Errorf("aws eks list-clusters failed: %w", err)
	}
	return parseListClustersJSON(out)
}

// ── Detección de duplicados ─────────────────────────────

// getExistingEKSContexts lee el kubeconfig actual y extrae los contextos
// que corresponden a clústeres EKS (contienen "arn:aws:eks:").
// Retorna un mapa donde las claves son los nombres de contexto EKS.
func getExistingEKSContexts() (map[string]bool, error) {
	out, err := exec.Command("kubectl", "config", "get-contexts", "-o", "name").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig contexts: %w", err)
	}

	contexts := make(map[string]bool)
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if strings.Contains(name, "arn:aws:eks:") {
			contexts[name] = true
		}
	}
	return contexts, nil
}

// buildClusterARN construye el ARN esperado para un clúster EKS.
// Formato: arn:aws:eks:<region>:<accountID>:cluster/<name>
func buildClusterARN(cluster, region, accountID string) string {
	return fmt.Sprintf("arn:aws:eks:%s:%s:cluster/%s", region, accountID, cluster)
}

// ── Sincronización del kubeconfig ───────────────────────

// updateKubeconfig ejecuta "aws eks update-kubeconfig" para agregar un clúster al kubeconfig.
// Retorna error si el comando falla.
// updateKubeconfig ejecuta "aws eks update-kubeconfig" para agregar un clúster.
// Si tmpFile no está vacío, escribe a ese archivo temporal en vez del kubeconfig principal.
func updateKubeconfig(cluster, profile, region, tmpFile string) error {
	args := []string{"eks", "update-kubeconfig",
		"--name", cluster,
		"--profile", profile,
		"--region", region,
	}
	if tmpFile != "" {
		args = append(args, "--kubeconfig", tmpFile)
	}
	cmd := exec.Command("aws", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to update kubeconfig for cluster '%s': %s", cluster, strings.TrimSpace(string(out)))
	}
	return nil
}
// mergeKubeconfigs mergea archivos kubeconfig temporales al kubeconfig principal.
// Usa KUBECONFIG=main:tmp1:tmp2:... kubectl config view --flatten > main
func mergeKubeconfigs(mainKubeconfig string, tmpFiles []string) error {
	if len(tmpFiles) == 0 {
		return nil
	}

	// Asegurar que el directorio ~/.kube/ exista
	if err := os.MkdirAll(filepath.Dir(mainKubeconfig), 0700); err != nil {
		return fmt.Errorf("failed to create kubeconfig directory: %w", err)
	}

	// Solo incluir mainKubeconfig en el env si el archivo ya existe
	var paths []string
	if _, err := os.Stat(mainKubeconfig); err == nil {
		paths = append(paths, mainKubeconfig)
	}
	paths = append(paths, tmpFiles...)
	kubeconfigEnv := strings.Join(paths, ":")

	cmd := exec.Command("kubectl", "config", "view", "--flatten")
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfigEnv)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to merge kubeconfigs: %w", err)
	}
	return os.WriteFile(mainKubeconfig, out, 0600)
}
// partitionClusters divide los clústeres descubiertos en nuevos y existentes.
// Un clúster es "existente" si algún contexto en el mapa existing contiene
// tanto el nombre del clúster como la región (e.g., el ARN del contexto).
// Retorna dos slices disjuntos cuya unión es igual al conjunto original.
func partitionClusters(discovered []eksCluster, existing map[string]bool) (newClusters, existingClusters []eksCluster) {
	for _, c := range discovered {
		found := false
		for ctx := range existing {
			if strings.Contains(ctx, c.Region) && strings.Contains(ctx, c.Name) {
				found = true
				break
			}
		}
		if found {
			existingClusters = append(existingClusters, c)
		} else {
			newClusters = append(newClusters, c)
		}
	}
	return newClusters, existingClusters
}



// ── Orquestadores ───────────────────────────────────────

// handleEksKubeconfig ejecuta la sincronización completa de clústeres EKS al kubeconfig.
// Si profileFilter no está vacío, solo procesa el perfil indicado.
func cliProgressBar(current, total, width int) string {
	if total == 0 {
		return strings.Repeat("░", width)
	}
	filled := current * width / total
	if filled > width {
		filled = width
	}
	cyanSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#00b4d8"))
	return cyanSt.Render(strings.Repeat("█", filled)) + dimStyle.Render(strings.Repeat("░", width-filled))
}

var cyanStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#00b4d8"))

func handleEksKubeconfig(profileFilter string) {
	fmt.Println(logoStyle.Render("⎈ ksw eks kubeconfig"))
	fmt.Println()

	// 1. Validar que AWS CLI está disponible
	if err := checkAWSCLI(); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	// 2. Parsear perfiles AWS
	profiles, err := parseAWSProfiles("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}
	if len(profiles) == 0 {
		fmt.Println(dimStyle.Render("No AWS profiles found in ~/.aws/config"))
		return
	}

	// 3. Filtrar por --profile si se proporcionó
	if profileFilter != "" {
		profiles, err = filterProfiles(profiles, profileFilter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
			os.Exit(1)
		}
	}

	// 4. Descubrir clústeres EKS por perfil (en paralelo, máx 32 concurrentes)
	const maxConcurrent = 32
	sem := make(chan struct{}, maxConcurrent)

	type profileResult struct {
		profile  awsProfile
		clusters []string
		err      error
	}
	resultsCh := make(chan profileResult, len(profiles))
	for _, p := range profiles {
		go func(p awsProfile) {
			sem <- struct{}{}
			clusters, err := listEKSClusters(p.Name, p.Region)
			<-sem
			resultsCh <- profileResult{profile: p, clusters: clusters, err: err}
		}(p)
	}

	total := len(profiles)
	remaining := total
	done := 0
	spinnerLabel := func() string {
		bar := cliProgressBar(done, total, 20)
		return fmt.Sprintf("Scanning profiles... %s %s",
			bar, dimStyle.Render(fmt.Sprintf("%d / %d", done, total)))
	}

	spinnerDone := make(chan struct{})
	labelCh := make(chan string, total)
	go func() {
		frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		i := 0
		label := spinnerLabel()
		for {
			select {
			case <-spinnerDone:
				fmt.Printf("\r\033[K")
				return
			case l := <-labelCh:
				label = l
			default:
				fmt.Printf("\r  %s %s", successStyle.Render(frames[i%len(frames)]), label)
				i++
				time.Sleep(80 * time.Millisecond)
			}
		}
	}()

	var allDiscovered []eksCluster

	for range profiles {
		r := <-resultsCh
		done++
		remaining--
		labelCh <- spinnerLabel()
		if r.err != nil {
			fmt.Printf("\r\033[K  %s profile '%s' [%s] — %s\n",
				warnStyle.Render("✗"), r.profile.Name, r.profile.Region, dimStyle.Render(r.err.Error()))
		} else if len(r.clusters) > 0 {
			fmt.Printf("\r\033[K  %s profile '%s' [%s] — %s\n",
				successStyle.Render("✔"), r.profile.Name, r.profile.Region,
				cyanStyle.Render(fmt.Sprintf("%d cluster(s) found", len(r.clusters))))
			for _, c := range r.clusters {
				allDiscovered = append(allDiscovered, eksCluster{Name: c, Profile: r.profile.Name, Region: r.profile.Region})
			}
		}
	}
	close(spinnerDone)
	fmt.Println()

	if len(allDiscovered) == 0 {
		fmt.Println(dimStyle.Render("  No clusters found across all profiles — check SSO session and profile regions."))
		return
	}

	// 5. Obtener contextos existentes del kubeconfig
	existing, err := getExistingEKSContexts()
	if err != nil {
		// Non-fatal: si no podemos leer contextos, tratamos todo como nuevo
		existing = make(map[string]bool)
	}

	// 6. Particionar clústeres nuevos vs existentes
	newClusters, existingClusters := partitionClusters(allDiscovered, existing)

	// 7. Sincronizar clústeres nuevos (paralelo con archivos temporales + merge final)
	type syncOutcome struct {
		cluster eksCluster
		tmpFile string
		err     error
	}

	// Determinar kubeconfig principal
	mainKubeconfig := os.Getenv("KUBECONFIG")
	if mainKubeconfig == "" {
		home, _ := os.UserHomeDir()
		mainKubeconfig = filepath.Join(home, ".kube", "config")
	}

	// Crear directorio temporal para kubeconfigs parciales
	tmpDir, err := os.MkdirTemp("", "ksw-eks-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Cannot create temp dir: %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

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

	var result syncResult
	var tmpFiles []string

	addedCount := 0
	totalNew := len(newClusters)
	addLabel := func() string {
		bar := cliProgressBar(addedCount, totalNew, 20)
		return fmt.Sprintf("Adding clusters... %s %s",
			bar, dimStyle.Render(fmt.Sprintf("%d / %d", addedCount, totalNew)))
	}
	syncSpinnerDone := make(chan struct{})
	addLabelCh := make(chan string, totalNew)
	go func() {
		frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		i := 0
		label := addLabel()
		for {
			select {
			case <-syncSpinnerDone:
				fmt.Printf("\r\033[K")
				return
			case l := <-addLabelCh:
				label = l
			default:
				fmt.Printf("\r  %s %s", successStyle.Render(frames[i%len(frames)]), label)
				i++
				time.Sleep(80 * time.Millisecond)
			}
		}
	}()

	for range newClusters {
		s := <-syncCh
		if s.err != nil {
			result.Failed++
			fmt.Printf("\r\033[K  %s failed %s — %s\n",
				warnStyle.Render("✗"), s.cluster.Name, dimStyle.Render(s.err.Error()))
		} else {
			addedCount++
			result.Added++
			tmpFiles = append(tmpFiles, s.tmpFile)
			fmt.Printf("\r\033[K  %s added %s  (%s)\n",
				cyanStyle.Render("✔"), s.cluster.Name, s.cluster.Profile)
		}
		addLabelCh <- addLabel()
	}
	close(syncSpinnerDone)

	// Merge todos los kubeconfigs temporales al principal
	if len(tmpFiles) > 0 {
		if err := mergeKubeconfigs(mainKubeconfig, tmpFiles); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s %s\n", warnStyle.Render("✗"), err)
		}
	}

	// Mostrar clústeres existentes omitidos
	for _, c := range existingClusters {
		fmt.Printf("  %s skipped %s (already in kubeconfig)\n",
			dimStyle.Render("·"), c.Name)
		result.Skipped++
	}

	// Resumen final
	fmt.Println()
	fmt.Printf("  %s Done — %s added, %s skipped, %s failed\n",
		successStyle.Render("✔"),
		successStyle.Render(fmt.Sprintf("%d", result.Added)),
		dimStyle.Render(fmt.Sprintf("%d", result.Skipped)),
		warnStyle.Render(fmt.Sprintf("%d", result.Failed)))
}

// handleEks es el entry point desde main(), enruta subcomandos de "ksw eks".
// Parsea os.Args para detectar subcomando "kubeconfig" y flag "--profile".
func handleEks() {
	args := os.Args[2:] // skip "ksw" and "eks"

	if len(args) == 0 {
		fmt.Println(logoStyle.Render("⎈ ksw eks"))
		fmt.Println()
		fmt.Println("  Usage:")
		fmt.Println("    ksw eks config                           Interactive TUI for kubeconfig management")
		fmt.Println("    ksw eks kubeconfig sync                  Sync EKS clusters → ~/.kube/config")
		fmt.Println("    ksw eks kubeconfig sync --profile <name> Sync only one AWS profile")
		fmt.Println()
		return
	}

	switch args[0] {
	case "config":
		handleEksTUI()

	case "kubeconfig":
		if len(args) < 2 {
			fmt.Println(logoStyle.Render("⎈ ksw eks kubeconfig"))
			fmt.Println()
			fmt.Println("  Usage:")
			fmt.Println("    ksw eks kubeconfig sync                  Sync EKS clusters → ~/.kube/config")
			fmt.Println("    ksw eks kubeconfig sync --profile <name> Sync only one AWS profile")
			fmt.Println()
			return
		}
		switch args[1] {
		case "sync":
			profileFilter := ""
			for i := 2; i < len(args); i++ {
				if args[i] == "--profile" && i+1 < len(args) {
					profileFilter = args[i+1]
					break
				}
			}
			handleEksKubeconfig(profileFilter)
		default:
			fmt.Fprintf(os.Stderr, "%s Unknown subcommand: ksw eks kubeconfig %s\n", warnStyle.Render("✗"), args[1])
			fmt.Fprintf(os.Stderr, "Run 'ksw eks kubeconfig' for usage.\n")
			os.Exit(1)
		}

	case "create-profiles":
		handleCreateProfiles()

	default:
		fmt.Fprintf(os.Stderr, "%s Unknown subcommand: ksw eks %s\n", warnStyle.Render("✗"), args[0])
		fmt.Fprintf(os.Stderr, "Run 'ksw eks' for usage.\n")
		os.Exit(1)
	}
}

var awsUsage = `%s

Usage:
  ksw aws sso config                      Configure AWS SSO sessions (TUI)
  ksw aws sso login                       Login to default SSO session
  ksw aws sso login <session>             Login to a specific SSO session
  ksw aws sso profiles list               List configured AWS profiles
  ksw aws sso profiles sync               Auto-sync SSO accounts to ~/.aws/config
  ksw aws sso profiles add <name> <id>    Add a single profile [--session <s>]
  ksw aws sso profiles search <term>      Search profiles by name or account ID
`

var ssoUsage = `%s

Usage:
  ksw aws sso config                      Configure AWS SSO sessions (TUI)
  ksw aws sso login                       Login to default SSO session
  ksw aws sso login <session>             Login to a specific SSO session
  ksw aws sso profiles list               List configured AWS profiles
  ksw aws sso profiles sync               Auto-sync SSO accounts to ~/.aws/config
  ksw aws sso profiles add <name> <id>    Add a single profile [--session <s>]
  ksw aws sso profiles search <term>      Search profiles by name or account ID
`

var ssoProfilesUsage = `%s

Usage:
  ksw aws sso profiles list               List configured AWS profiles
  ksw aws sso profiles sync               Auto-sync SSO accounts to ~/.aws/config
  ksw aws sso profiles add <name> <id>    Add a single profile [--session <s>]
  ksw aws sso profiles search <term>      Search profiles by name or account ID
`

func handleAWS() {
	args := os.Args[2:] // skip "ksw" and "aws"

	if len(args) == 0 {
		fmt.Printf(awsUsage, logoStyle.Render("⎈ ksw aws"))
		return
	}

	switch args[0] {
	case "sso":
		handleAWSSSO(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "%s Unknown subcommand: ksw aws %s\n", warnStyle.Render("✗"), args[0])
		fmt.Fprintf(os.Stderr, "Run 'ksw aws' for usage.\n")
		os.Exit(1)
	}
}

func handleAWSSSO(args []string) {
	if len(args) == 0 {
		fmt.Printf(ssoUsage, logoStyle.Render("⎈ ksw aws sso"))
		return
	}

	// ── Premium gate ──────────────────────────────────────
	if !checkPremiumAccess() {
		buyURL := "https://ksw.lemonsqueezy.com/checkout/buy/5b89e2bc-9b58-4343-84d3-2dcbf22d67a1"
		// OSC 8 hyperlink — clickeable en iTerm2, Terminal.app, VSCode
		clickable := "\033]8;;" + buyURL + "\033\\" + buyURL + "\033]8;;\033\\"
		fmt.Println(logoStyle.Render("⎈ ksw aws sso") + "  " + warnStyle.Render("[ premium ]"))
		fmt.Println()
		fmt.Println("  " + warnStyle.Render("✗") + " This feature requires a valid license.")
		fmt.Println()
		fmt.Println("  " + dimStyle.Render("Activate with: ksw license activate <your-key>"))
		fmt.Println("  " + dimStyle.Render("Get a license: "+clickable))
		fmt.Println("  " + dimStyle.Render("Or run:        ksw license buy"))
		os.Exit(1)
	}

	switch args[0] {
	case "config":
		handleSSOConfig()
	case "login":
		handleSSOLogin()
	case "profiles":
		handleAWSSSO_Profiles(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "%s Unknown subcommand: ksw aws sso %s\n", warnStyle.Render("✗"), args[0])
		fmt.Fprintf(os.Stderr, "Run 'ksw aws sso' for usage.\n")
		os.Exit(1)
	}
}

func handleAWSSSO_Profiles(args []string) {
	if len(args) == 0 {
		fmt.Printf(ssoProfilesUsage, logoStyle.Render("⎈ ksw aws sso profiles"))
		return
	}

	switch args[0] {
	case "list", "ls":
		handleListProfiles()
	case "sync":
		handleCreateProfiles()
	case "add":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "%s Usage: ksw aws sso profiles add <name> <account-id> [--session <s>]\n", warnStyle.Render("✗"))
			os.Exit(1)
		}
		session := ""
		for i := 3; i < len(args); i++ {
			if args[i] == "--session" && i+1 < len(args) {
				session = args[i+1]
				break
			}
		}
		handleAddProfile(args[1], args[2], session)
	case "search":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%s Usage: ksw aws sso profiles search <term>\n", warnStyle.Render("✗"))
			os.Exit(1)
		}
		handleSearchProfiles(args[1])
	default:
		fmt.Fprintf(os.Stderr, "%s Unknown subcommand: ksw aws sso profiles %s\n", warnStyle.Render("✗"), args[0])
		fmt.Fprintf(os.Stderr, "Run 'ksw aws sso profiles' for usage.\n")
		os.Exit(1)
	}
}
