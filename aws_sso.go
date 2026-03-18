package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── Estructuras de datos AWS SSO ──────────────────────

type ssoAccount struct {
	AccountID   string `json:"accountId"`
	AccountName string `json:"accountName"`
}

type ssoListResponse struct {
	AccountList []ssoAccount `json:"accountList"`
}

// ── Configuración por defecto ──────────────────────────

// loadSSOSettings retorna la config SSO para la sesión indicada (o la default).
func loadSSOSettings() ssoConfig {
	return loadSSOSettingsFor("")
}

// loadSSOSettingsFor retorna la config SSO para una sesión específica.
// Si sessionName está vacío, usa la sesión default.
func loadSSOSettingsFor(sessionName string) ssoConfig {
	cfg := loadConfig()

	// Migrar formato viejo (campo SSO único) al nuevo (mapa de sesiones)
	if cfg.SSOSessions == nil {
		cfg.SSOSessions = make(map[string]ssoConfig)
	}
	if cfg.SSO.SessionName != "" && len(cfg.SSOSessions) == 0 {
		cfg.SSOSessions[cfg.SSO.SessionName] = cfg.SSO
		if cfg.SSODefault == "" {
			cfg.SSODefault = cfg.SSO.SessionName
		}
		cfg.SSO = ssoConfig{} // limpiar campo viejo
		_ = saveConfig(cfg)
	}

	// Determinar qué sesión usar
	name := sessionName
	if name == "" {
		name = cfg.SSODefault
	}

	if s, ok := cfg.SSOSessions[name]; ok {
		return applyDefaults(s)
	}

	// Si no hay nada configurado, retornar defaults hardcodeados
	return ssoConfig{
		SessionName: "bco",
		StartURL:    "https://d-9067080964.awsapps.com/start/#",
		Region:      "us-east-1",
		RoleName:    "BCO-SysOpsEngineerRole",
	}
}

func applyDefaults(s ssoConfig) ssoConfig {
	if s.SessionName == "" {
		s.SessionName = "bco"
	}
	if s.StartURL == "" {
		s.StartURL = "https://d-9067080964.awsapps.com/start/#"
	}
	if s.Region == "" {
		s.Region = "us-east-1"
	}
	if s.RoleName == "" {
		s.RoleName = "BCO-SysOpsEngineerRole"
	}
	return s
}

// saveSSOSession guarda una sesión SSO en el mapa y opcionalmente la marca como default.
func saveSSOSession(name string, s ssoConfig, setDefault bool) error {
	cfg := loadConfig()
	if cfg.SSOSessions == nil {
		cfg.SSOSessions = make(map[string]ssoConfig)
	}
	cfg.SSOSessions[name] = s
	if setDefault || cfg.SSODefault == "" {
		cfg.SSODefault = name
	}
	cfg.SSO = ssoConfig{} // limpiar campo viejo
	return saveConfig(cfg)
}

// listSSOSessions retorna los nombres de sesiones configuradas y cuál es la default.
// También migra el formato viejo si es necesario.
func listSSOSessions() ([]string, string) {
	cfg := loadConfig()

	// Migrar formato viejo si existe
	if cfg.SSOSessions == nil {
		cfg.SSOSessions = make(map[string]ssoConfig)
	}
	if cfg.SSO.SessionName != "" && len(cfg.SSOSessions) == 0 {
		cfg.SSOSessions[cfg.SSO.SessionName] = cfg.SSO
		if cfg.SSODefault == "" {
			cfg.SSODefault = cfg.SSO.SessionName
		}
		cfg.SSO = ssoConfig{}
		_ = saveConfig(cfg)
	}

	if len(cfg.SSOSessions) == 0 {
		return nil, ""
	}
	names := make([]string, 0, len(cfg.SSOSessions))
	for n := range cfg.SSOSessions {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, cfg.SSODefault
}

func awsConfigPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".aws", "config")
}

// ── Parseo de perfiles ─────────────────────────────────

func parseProfilesWithAccountID() (valid map[string]string, malformed map[string]string, err error) {
	configFile := awsConfigPath()
	f, err := os.Open(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, map[string]string{}, nil
		}
		return nil, nil, fmt.Errorf("cannot read %s: %w", configFile, err)
	}
	defer f.Close()

	valid = make(map[string]string)
	malformed = make(map[string]string)
	profileRe := regexp.MustCompile(`^\[profile\s+([^\]]+)\]$`)

	var cur string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if m := profileRe.FindStringSubmatch(line); m != nil {
			cur = strings.TrimSpace(m[1])
			continue
		}
		if cur != "" && strings.HasPrefix(line, "sso_account_id") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				id := strings.TrimSpace(parts[1])
				if isValidAccountID(id) {
					valid[cur] = id
				} else {
					malformed[cur] = id
				}
				cur = ""
			}
		}
	}
	return valid, malformed, sc.Err()
}

func isValidAccountID(id string) bool {
	if len(id) != 12 {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ── Obtener cuentas desde AWS SSO ──────────────────────

func loadSSOAccounts() ([]ssoAccount, error) {
	ssoCfg := loadSSOSettings()
	cmd := exec.Command("aws", "sso", "list-accounts", "--region", ssoCfg.Region)
	cmd.Env = append(os.Environ(), "AWS_PAGER=")
	out, err := cmd.Output()
	if err == nil {
		var resp ssoListResponse
		if err := json.Unmarshal(out, &resp); err == nil && len(resp.AccountList) > 0 {
			return resp.AccountList, nil
		}
	}

	// Fallback: token desde cache
	home, _ := os.UserHomeDir()
	cacheDir := filepath.Join(home, ".aws", "sso", "cache")
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("SSO cache not found. Run: aws sso login --sso-session %s", ssoCfg.SessionName)
	}

	var token string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, _ := os.ReadFile(filepath.Join(cacheDir, e.Name()))
		var cache map[string]interface{}
		if json.Unmarshal(data, &cache) != nil {
			continue
		}
		if t, ok := cache["accessToken"].(string); ok && t != "" {
			token = t
			break
		}
	}
	if token == "" {
		return nil, fmt.Errorf("no valid SSO token. Run: aws sso login --sso-session %s", ssoCfg.SessionName)
	}

	cmd = exec.Command("aws", "sso", "list-accounts", "--access-token", token, "--region", ssoCfg.Region)
	cmd.Env = append(os.Environ(), "AWS_PAGER=")
	out, err = cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list SSO accounts. Run: aws sso login --sso-session %s", ssoCfg.SessionName)
	}
	var resp ssoListResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse SSO response: %w", err)
	}
	return resp.AccountList, nil
}


// ── Normalización de nombres ───────────────────────────

func normalizeAccountName(name string) string {
	re1 := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	s := re1.ReplaceAllString(name, "${1}-${2}")

	re2 := regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
	s = re2.ReplaceAllString(s, "${1}-${2}")

	re3 := regexp.MustCompile(`(?i)([A-Z]{2,})(QA|PDN|PRD|PROD|SBX|LAB)` + `\b`)
	s = re3.ReplaceAllString(s, "${1}-${2}")

	s = strings.ToLower(s)

	knownPatterns := [][2]string{
		{`(ciam|ztna)(dev|qa|pdn|prod|sbx|lab)` + `\b`, "${1}-${2}"},
		{`(ti)(dev|qa|pdn|prod|sbx|lab)` + `\b`, "ti-${2}"},
		{`(wi)(azure)`, "${1}-${2}"},
		{`(xspm)(dev|qa|pdn|prod)` + `\b`, "xspm-${2}"},
		{`(pdti)(lab)`, "pdti-${2}"},
		{`(non)(prod)` + `\b`, "non-${2}"},
	}
	for _, p := range knownPatterns {
		re := regexp.MustCompile(p[0])
		s = re.ReplaceAllString(s, p[1])
	}

	s = regexp.MustCompile(`[^a-z0-9-]`).ReplaceAllString(s, "-")
	s = regexp.MustCompile(`-+`).ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

// ── Backup ─────────────────────────────────────────────

func createAWSConfigBackup() (string, error) {
	src := awsConfigPath()
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return "", nil
	}
	ts := time.Now().Format("20060102_150405")
	dst := src + ".backup." + ts
	data, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return "", err
	}
	return dst, nil
}

// ── Ensure SSO session ─────────────────────────────────

func ensureSSOSession() error {
	cfg := loadSSOSettings()
	return ensureSSOSessionFor(cfg)
}

func ensureSSOSessionFor(cfg ssoConfig) error {
	configFile := awsConfigPath()

	// Crear directorio si no existe
	dir := filepath.Dir(configFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			content := fmt.Sprintf("[sso-session %s]\nsso_start_url = %s\nsso_region = %s\n\n[default]\nregion = %s\n",
				cfg.SessionName, cfg.StartURL, cfg.Region, cfg.Region)
			return os.WriteFile(configFile, []byte(content), 0644)
		}
		return err
	}

	if !strings.Contains(string(data), "[sso-session "+cfg.SessionName+"]") {
		header := fmt.Sprintf("[sso-session %s]\nsso_start_url = %s\nsso_region = %s\n\n",
			cfg.SessionName, cfg.StartURL, cfg.Region)
		return os.WriteFile(configFile, []byte(header+string(data)), 0644)
	}
	return nil
}

// upsertSSOSessionInConfig adds or updates [sso-session NAME] in ~/.aws/config
func upsertSSOSessionInConfig(cfg ssoConfig) error {
	configFile := awsConfigPath()
	if err := os.MkdirAll(filepath.Dir(configFile), 0755); err != nil {
		return err
	}

	raw, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			content := fmt.Sprintf("[sso-session %s]\nsso_start_url = %s\nsso_region = %s\n\n",
				cfg.SessionName, cfg.StartURL, cfg.Region)
			return os.WriteFile(configFile, []byte(content), 0644)
		}
		return err
	}

	lines := strings.Split(string(raw), "\n")
	header := "[sso-session " + cfg.SessionName + "]"
	inSection := false
	replaced := false
	var out []string

	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if strings.TrimSpace(l) == header {
			inSection = true
			replaced = true
			// Write the updated block
			out = append(out, l)
			out = append(out, "sso_start_url = "+cfg.StartURL)
			out = append(out, "sso_region = "+cfg.Region)
			// Skip old key=value lines until next section or EOF
			for i+1 < len(lines) {
				next := strings.TrimSpace(lines[i+1])
				if strings.HasPrefix(next, "[") || next == "" {
					break
				}
				i++
			}
			inSection = false
			continue
		}
		out = append(out, l)
	}

	if !replaced {
		// Append new section at top
		block := fmt.Sprintf("[sso-session %s]\nsso_start_url = %s\nsso_region = %s\n\n",
			cfg.SessionName, cfg.StartURL, cfg.Region)
		content := block + strings.Join(out, "\n")
		return os.WriteFile(configFile, []byte(content), 0644)
	}
	_ = inSection
	return os.WriteFile(configFile, []byte(strings.Join(out, "\n")), 0644)
}

// countProfilesForSession cuenta cuántos [profile X] en ~/.aws/config referencian la sesión
func countProfilesForSession(sessionName string) int {
	configFile := awsConfigPath()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		return 0
	}
	count := 0
	currentIsProfile := false
	for _, l := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "[profile ") {
			currentIsProfile = true
		} else if strings.HasPrefix(trimmed, "[") {
			currentIsProfile = false
		}
		if currentIsProfile && strings.HasPrefix(trimmed, "sso_session") {
			parts := strings.SplitN(trimmed, "=", 2)
			if len(parts) == 2 && strings.TrimSpace(parts[1]) == sessionName {
				count++
			}
		}
	}
	return count
}

// removeSSOSessionBlockOnly elimina solo el bloque [sso-session NAME] de ~/.aws/config,
// sin tocar los [profile] que lo referencian.
func removeSSOSessionBlockOnly(sessionName string) error {
	configFile := awsConfigPath()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	lines := strings.Split(string(raw), "\n")
	var out []string
	skip := false
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "[sso-session "+sessionName+"]" {
			skip = true
			continue
		}
		if skip && strings.HasPrefix(trimmed, "[") {
			skip = false
		}
		if !skip {
			out = append(out, l)
		}
	}
	// Limpiar líneas vacías consecutivas
	var clean []string
	prevEmpty := false
	for _, l := range out {
		isEmpty := strings.TrimSpace(l) == ""
		if isEmpty && prevEmpty {
			continue
		}
		clean = append(clean, l)
		prevEmpty = isEmpty
	}
	for len(clean) > 0 && strings.TrimSpace(clean[0]) == "" {
		clean = clean[1:]
	}
	return os.WriteFile(configFile, []byte(strings.Join(clean, "\n")), 0644)
}

// removeSSOSessionFromConfig elimina de ~/.aws/config:
//   - el bloque [sso-session NAME]
//   - todos los [profile X] que referencian esa sesión vía sso_session = NAME
func removeSSOSessionFromConfig(sessionName string) error {
	configFile := awsConfigPath()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Primer paso: identificar qué profiles referencian esta sesión
	profilesToRemove := make(map[string]bool)
	lines := strings.Split(string(raw), "\n")
	currentSection := ""
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "[profile ") && strings.HasSuffix(trimmed, "]") {
			currentSection = trimmed
		} else if trimmed == "[sso-session "+sessionName+"]" {
			currentSection = trimmed
		} else if strings.HasPrefix(trimmed, "[") {
			currentSection = trimmed
		}
		if strings.HasPrefix(trimmed, "sso_session") {
			parts := strings.SplitN(trimmed, "=", 2)
			if len(parts) == 2 && strings.TrimSpace(parts[1]) == sessionName {
				profilesToRemove[currentSection] = true
			}
		}
	}

	// Segundo paso: reescribir el archivo omitiendo las secciones marcadas
	var out []string
	skip := false
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)

		// Detectar inicio de sección
		if strings.HasPrefix(trimmed, "[") {
			isSSOSession := trimmed == "[sso-session "+sessionName+"]"
			isProfileToRemove := profilesToRemove[trimmed]
			skip = isSSOSession || isProfileToRemove
			if skip {
				continue
			}
		} else if skip {
			continue
		}

		out = append(out, l)
	}

	// Limpiar líneas vacías duplicadas consecutivas
	var clean []string
	prevEmpty := false
	for _, l := range out {
		isEmpty := strings.TrimSpace(l) == ""
		if isEmpty && prevEmpty {
			continue
		}
		clean = append(clean, l)
		prevEmpty = isEmpty
	}
	// Quitar línea vacía al inicio
	for len(clean) > 0 && strings.TrimSpace(clean[0]) == "" {
		clean = clean[1:]
	}

	return os.WriteFile(configFile, []byte(strings.Join(clean, "\n")), 0644)
}

// ── Agregar perfil ─────────────────────────────────────

func addProfileToConfig(profileName, accountID string) error {
	return addProfileToConfigWith(profileName, accountID, loadSSOSettings())
}

func addProfileToConfigWith(profileName, accountID string, cfg ssoConfig) error {
	if !isValidAccountID(accountID) {
		return fmt.Errorf("invalid account ID: %s", accountID)
	}
	configFile := awsConfigPath()
	data, _ := os.ReadFile(configFile)
	if strings.Contains(string(data), "[profile "+profileName+"]") {
		return fmt.Errorf("profile %s already exists", profileName)
	}

	block := fmt.Sprintf("\n[profile %s]\nsso_session = %s\nsso_account_id = %s\nsso_role_name = %s\nregion = %s\noutput = json\n",
		profileName, cfg.SessionName, accountID, cfg.RoleName, cfg.Region)

	f, err := os.OpenFile(configFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(block)
	return err
}

// ── Renombrar perfil ───────────────────────────────────

func renameProfileInConfig(oldName, newName string) error {
	configFile := awsConfigPath()
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	content := string(data)
	old := "[profile " + oldName + "]"
	if !strings.Contains(content, old) {
		return fmt.Errorf("profile %s not found", oldName)
	}
	content = strings.Replace(content, old, "[profile "+newName+"]", 1)
	return os.WriteFile(configFile, []byte(content), 0644)
}

// ── Actualizar account ID ──────────────────────────────

func updateAccountIDInConfig(profileName, oldID, newID string) error {
	configFile := awsConfigPath()
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	pattern := regexp.MustCompile(`(\[profile ` + regexp.QuoteMeta(profileName) + `\][^\[]*?sso_account_id\s*=\s*)\d+`)
	result := pattern.ReplaceAllString(string(data), "${1}"+newID)
	if result == string(data) {
		return fmt.Errorf("could not locate profile %s for update", profileName)
	}
	return os.WriteFile(configFile, []byte(result), 0644)
}


// ── Comando: ksw eks create-profiles (auto sync) ──────

func handleCreateProfiles() {
	fmt.Println(logoStyle.Render("⎈ ksw eks create-profiles"))
	fmt.Println()

	if err := checkAWSCLI(); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	if err := ensureSSOSession(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Cannot ensure SSO session: %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	// Obtener cuentas SSO
	fmt.Printf("  %s Fetching SSO accounts...\n", dimStyle.Render("⟳"))
	ssoAccounts, err := loadSSOAccounts()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}
	fmt.Printf("  %s %d accounts from SSO\n", successStyle.Render("✔"), len(ssoAccounts))

	// Parsear perfiles actuales
	profiles, malformed, err := parseProfilesWithAccountID()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}
	fmt.Printf("  %s %d profiles configured", successStyle.Render("✔"), len(profiles))
	if len(malformed) > 0 {
		fmt.Printf(" (%s malformed)", warnStyle.Render(fmt.Sprintf("%d", len(malformed))))
	}
	fmt.Println()

	// Construir mapas de SSO
	ssoByNorm := make(map[string]ssoAccount) // normalized name -> account
	ssoByID := make(map[string]string)        // accountID -> normalized name
	for _, a := range ssoAccounts {
		norm := normalizeAccountName(a.AccountName)
		ssoByNorm[norm] = a
		ssoByID[a.AccountID] = norm
	}

	needsBackup := false
	var renames [][2]string // old, new
	var idFixes [][3]string // profile, oldID, newID

	// Detectar perfiles con nombres incorrectos
	for name, id := range profiles {
		if expected, ok := ssoByID[id]; ok && expected != name {
			renames = append(renames, [2]string{name, expected})
			needsBackup = true
		}
	}

	// Detectar perfiles con account ID incorrecto
	for name, id := range profiles {
		if expected, ok := ssoByNorm[name]; ok && expected.AccountID != id {
			idFixes = append(idFixes, [3]string{name, id, expected.AccountID})
			needsBackup = true
		}
	}

	// Detectar cuentas faltantes
	existingIDs := make(map[string]bool)
	existingNames := make(map[string]bool)
	for name, id := range profiles {
		existingIDs[id] = true
		existingNames[name] = true
	}
	var missing []ssoAccount
	for _, a := range ssoAccounts {
		if !existingIDs[a.AccountID] {
			missing = append(missing, a)
			needsBackup = true
		}
	}

	// Si no hay cambios
	if !needsBackup {
		fmt.Println()
		fmt.Printf("%s All %d SSO accounts are already configured\n",
			successStyle.Render("✔"), len(ssoAccounts))
		return
	}

	// Crear backup
	backup, err := createAWSConfigBackup()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Backup failed: %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}
	if backup != "" {
		fmt.Printf("  %s Backup: %s\n", dimStyle.Render("·"), backup)
	}
	fmt.Println()

	// Aplicar renombramientos
	if len(renames) > 0 {
		fmt.Printf("  %s Fixing %d profile names:\n", dimStyle.Render("⟳"), len(renames))
		for _, r := range renames {
			if err := renameProfileInConfig(r[0], r[1]); err != nil {
				fmt.Printf("    %s %s → %s: %s\n", warnStyle.Render("✗"), r[0], r[1], err)
			} else {
				fmt.Printf("    %s %s → %s\n", successStyle.Render("✔"), r[0], r[1])
			}
		}
		// Recargar perfiles
		profiles, malformed, _ = parseProfilesWithAccountID()
		existingIDs = make(map[string]bool)
		existingNames = make(map[string]bool)
		for name, id := range profiles {
			existingIDs[id] = true
			existingNames[name] = true
		}
		fmt.Println()
	}

	// Aplicar correcciones de account ID
	if len(idFixes) > 0 {
		fmt.Printf("  %s Fixing %d account IDs:\n", dimStyle.Render("⟳"), len(idFixes))
		for _, fix := range idFixes {
			if err := updateAccountIDInConfig(fix[0], fix[1], fix[2]); err != nil {
				fmt.Printf("    %s %s: %s\n", warnStyle.Render("✗"), fix[0], err)
			} else {
				fmt.Printf("    %s %s: %s → %s\n", successStyle.Render("✔"), fix[0], fix[1], fix[2])
			}
		}
		// Recargar
		profiles, _, _ = parseProfilesWithAccountID()
		existingIDs = make(map[string]bool)
		existingNames = make(map[string]bool)
		for name, id := range profiles {
			existingIDs[id] = true
			existingNames[name] = true
		}
		// Recalcular missing
		missing = nil
		for _, a := range ssoAccounts {
			if !existingIDs[a.AccountID] {
				missing = append(missing, a)
			}
		}
		fmt.Println()
	}

	// Corregir malformed account IDs
	if len(malformed) > 0 {
		fmt.Printf("  %s Fixing %d malformed account IDs:\n", dimStyle.Render("⟳"), len(malformed))
		for name, badID := range malformed {
			if expected, ok := ssoByNorm[name]; ok {
				if err := updateAccountIDInConfig(name, badID, expected.AccountID); err != nil {
					fmt.Printf("    %s %s: %s\n", warnStyle.Render("✗"), name, err)
				} else {
					fmt.Printf("    %s %s: %s → %s\n", successStyle.Render("✔"), name, badID, expected.AccountID)
				}
			} else {
				fmt.Printf("    %s %s: no SSO match for malformed ID %s\n", warnStyle.Render("✗"), name, badID)
			}
		}
		// Recargar y recalcular
		profiles, _, _ = parseProfilesWithAccountID()
		existingIDs = make(map[string]bool)
		for _, id := range profiles {
			existingIDs[id] = true
		}
		existingNames = make(map[string]bool)
		for name := range profiles {
			existingNames[name] = true
		}
		missing = nil
		for _, a := range ssoAccounts {
			if !existingIDs[a.AccountID] {
				missing = append(missing, a)
			}
		}
		fmt.Println()
	}

	// Agregar cuentas faltantes
	added := 0
	skipped := 0
	if len(missing) > 0 {
		fmt.Printf("  %s Adding %d missing profiles:\n", dimStyle.Render("⟳"), len(missing))
		for _, a := range missing {
			name := normalizeAccountName(a.AccountName)
			if existingNames[name] || existingIDs[a.AccountID] {
				fmt.Printf("    %s %s (%s) — skipped (conflict)\n", dimStyle.Render("·"), name, a.AccountID)
				skipped++
				continue
			}
			if err := addProfileToConfig(name, a.AccountID); err != nil {
				fmt.Printf("    %s %s: %s\n", warnStyle.Render("✗"), name, err)
				skipped++
			} else {
				fmt.Printf("    %s %s (%s)\n", successStyle.Render("✔"), name, a.AccountID)
				existingNames[name] = true
				existingIDs[a.AccountID] = true
				added++
			}
		}
		fmt.Println()
	}

	// Resumen
	finalProfiles, _, _ := parseProfilesWithAccountID()
	fmt.Printf("Done: %s added, %s fixed, %s skipped, %s total profiles\n",
		successStyle.Render(fmt.Sprintf("%d", added)),
		successStyle.Render(fmt.Sprintf("%d", len(renames)+len(idFixes))),
		dimStyle.Render(fmt.Sprintf("%d", skipped)),
		successStyle.Render(fmt.Sprintf("%d", len(finalProfiles))))
}


// ── Sync profiles: versión que retorna resultados (para TUI) ──

type syncProfileLine struct {
	ok   bool
	text string
}

type syncProfilesResult struct {
	lines []syncProfileLine
	added int
	fixed int
	total int
	err   error
}

// syncProfilesMsgResult es el tea.Msg que regresa el resultado del sync
type syncProfilesMsgResult struct {
	result syncProfilesResult
}

// syncProfilesProgressMsg envía una línea de progreso en vivo al TUI
type syncProfilesProgressMsg struct{ line syncProfileLine }

// drainSyncProfilesCh lee una línea o el resultado final del canal
func drainSyncProfilesCh(progressCh <-chan syncProfileLine, resultCh <-chan syncProfilesResult) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-progressCh
		if !ok {
			return syncProfilesMsgResult{result: <-resultCh}
		}
		return syncProfilesProgressMsg{line: line}
	}
}

// runSyncProfilesForSession ejecuta el sync de profiles en memoria (1 lectura, 1 escritura)
// y envía líneas de progreso en vivo al canal.
func runSyncProfilesForSession(sessionName string) syncProfilesResult {
	progressCh := make(chan syncProfileLine, 512)
	go func() {
		for range progressCh {
		}
	}()
	result := runSyncProfilesStreaming(sessionName, progressCh)
	close(progressCh)
	return result
}

func runSyncProfilesStreaming(sessionName string, progressCh chan<- syncProfileLine) syncProfilesResult {
	var res syncProfilesResult

	send := func(ok bool, msg string) {
		line := syncProfileLine{ok, msg}
		res.lines = append(res.lines, line)
		progressCh <- line
	}

	// Obtener la sesión SSO correcta
	cfg := loadConfig()
	var ssoCfg ssoConfig
	if s, ok := cfg.SSOSessions[sessionName]; ok {
		ssoCfg = s
	} else {
		ssoCfg = loadSSOSettings()
	}

	// Única llamada de red
	accounts, err := loadSSOAccounts()
	if err != nil {
		res.err = err
		return res
	}

	// ── Leer ~/.aws/config UNA SOLA VEZ ──────────────────
	configFile := awsConfigPath()
	rawData, err := os.ReadFile(configFile)
	if err != nil && !os.IsNotExist(err) {
		res.err = fmt.Errorf("cannot read %s: %w", configFile, err)
		return res
	}
	content := string(rawData)

	// Parsear profiles actuales desde el contenido en memoria
	profiles, malformed := parseProfilesFromContent(content)

	// Construir mapas de lookup
	ssoByNorm := make(map[string]ssoAccount)
	ssoByID := make(map[string]string)
	for _, a := range accounts {
		norm := normalizeAccountName(a.AccountName)
		ssoByNorm[norm] = a
		ssoByID[a.AccountID] = norm
	}

	existingIDs := make(map[string]bool)
	existingNames := make(map[string]bool)
	for name, id := range profiles {
		existingIDs[id] = true
		existingNames[name] = true
	}

	profileRe := regexp.MustCompile(`\[profile (\S+)\]`)
	accountIDRe := regexp.MustCompile(`(sso_account_id\s*=\s*)\S+`)

	// ── Fase 1: renombrar profiles con nombre incorrecto ─
	for name, id := range profiles {
		expected, ok := ssoByID[id]
		if !ok || expected == name {
			continue
		}
		old := "[profile " + name + "]"
		if !strings.Contains(content, old) {
			continue
		}
		content = strings.Replace(content, old, "[profile "+expected+"]", 1)
		send(true, fmt.Sprintf("renamed %s → %s", name, expected))
		res.fixed++
		delete(existingNames, name)
		existingNames[expected] = true
	}

	// ── Fase 2: corregir account IDs incorrectos ─────────
	// Re-parsear desde el contenido actualizado
	profiles, malformed = parseProfilesFromContent(content)
	existingIDs = make(map[string]bool)
	for _, id := range profiles {
		existingIDs[id] = true
	}

	// Construir secciones por profile para reemplazar account_id
	for name, id := range profiles {
		expected, ok := ssoByNorm[name]
		if !ok || expected.AccountID == id {
			continue
		}
		// Reemplazar dentro del bloque de este profile específico
		profileHeader := "[profile " + name + "]"
		idx := strings.Index(content, profileHeader)
		if idx == -1 {
			continue
		}
		// Encontrar fin del bloque (siguiente [section] o EOF)
		rest := content[idx:]
		nextSection := profileRe.FindStringIndex(rest[len(profileHeader):])
		var blockEnd int
		if nextSection != nil {
			blockEnd = idx + len(profileHeader) + nextSection[0]
		} else {
			blockEnd = len(content)
		}
		block := content[idx:blockEnd]
		newBlock := accountIDRe.ReplaceAllString(block, "${1}"+expected.AccountID)
		if newBlock == block {
			continue
		}
		content = content[:idx] + newBlock + content[blockEnd:]
		send(true, fmt.Sprintf("fixed id %s (%s → %s)", name, id, expected.AccountID))
		res.fixed++
		existingIDs[expected.AccountID] = true
		delete(existingIDs, id)
	}

	// ── Fase 3: corregir malformed ────────────────────────
	for name, badID := range malformed {
		expected, ok := ssoByNorm[name]
		if !ok {
			continue
		}
		profileHeader := "[profile " + name + "]"
		idx := strings.Index(content, profileHeader)
		if idx == -1 {
			continue
		}
		rest := content[idx:]
		nextSection := profileRe.FindStringIndex(rest[len(profileHeader):])
		var blockEnd int
		if nextSection != nil {
			blockEnd = idx + len(profileHeader) + nextSection[0]
		} else {
			blockEnd = len(content)
		}
		block := content[idx:blockEnd]
		newBlock := accountIDRe.ReplaceAllString(block, "${1}"+expected.AccountID)
		if newBlock == block {
			continue
		}
		content = content[:idx] + newBlock + content[blockEnd:]
		send(true, fmt.Sprintf("fixed malformed %s (%s → %s)", name, badID, expected.AccountID))
		res.fixed++
		existingIDs[expected.AccountID] = true
	}

	// ── Fase 4: agregar profiles faltantes ────────────────
	// Re-parsear para tener los IDs actualizados
	profiles, _ = parseProfilesFromContent(content)
	existingIDs = make(map[string]bool)
	existingNames = make(map[string]bool)
	for name, id := range profiles {
		existingIDs[id] = true
		existingNames[name] = true
	}

	var newBlocks strings.Builder
	for _, a := range accounts {
		if existingIDs[a.AccountID] {
			continue
		}
		name := normalizeAccountName(a.AccountName)
		if existingNames[name] {
			send(false, fmt.Sprintf("skip %s — name conflict", name))
			continue
		}
		if !isValidAccountID(a.AccountID) {
			send(false, fmt.Sprintf("skip %s — invalid account ID: %s", name, a.AccountID))
			continue
		}
		newBlocks.WriteString(fmt.Sprintf("\n[profile %s]\nsso_session = %s\nsso_account_id = %s\nsso_role_name = %s\nregion = %s\noutput = json\n",
			name, ssoCfg.SessionName, a.AccountID, ssoCfg.RoleName, ssoCfg.Region))
		send(true, fmt.Sprintf("added %s (%s)", name, a.AccountID))
		res.added++
		existingIDs[a.AccountID] = true
		existingNames[name] = true
	}
	content += newBlocks.String()

	// ── Escribir ~/.aws/config UNA SOLA VEZ ──────────────
	if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
		res.err = fmt.Errorf("failed to write %s: %w", configFile, err)
		return res
	}

	// Contar total final sin leer el archivo (ya lo tenemos en memoria)
	finalProfiles, _ := parseProfilesFromContent(content)
	res.total = len(finalProfiles)
	return res
}

// parseProfilesFromContent parsea profiles desde un string (sin I/O).
func parseProfilesFromContent(content string) (valid map[string]string, malformed map[string]string) {
	valid = make(map[string]string)
	malformed = make(map[string]string)
	profileRe := regexp.MustCompile(`^\[profile\s+([^\]]+)\]$`)
	var cur string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if m := profileRe.FindStringSubmatch(line); m != nil {
			cur = strings.TrimSpace(m[1])
			continue
		}
		if cur != "" && strings.HasPrefix(line, "sso_account_id") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				id := strings.TrimSpace(parts[1])
				if isValidAccountID(id) {
					valid[cur] = id
				} else {
					malformed[cur] = id
				}
				cur = ""
			}
		}
	}
	return
}

// ── Comando: ksw eks list-profiles ─────────────────────

func handleListProfiles() {
	fmt.Println(logoStyle.Render("⎈ ksw eks list-profiles"))
	fmt.Println()

	profiles, malformed, err := parseProfilesWithAccountID()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	if len(profiles) == 0 && len(malformed) == 0 {
		fmt.Println(dimStyle.Render("No profiles found in ~/.aws/config"))
		return
	}

	// Agrupar por ambiente
	envs := map[string][][2]string{
		"dev": {}, "qa": {}, "pdn": {}, "prod": {}, "other": {},
	}
	envOrder := []string{"dev", "qa", "pdn", "prod", "other"}

	for name, id := range profiles {
		lower := strings.ToLower(name)
		switch {
		case strings.Contains(lower, "dev"):
			envs["dev"] = append(envs["dev"], [2]string{name, id})
		case strings.Contains(lower, "qa"):
			envs["qa"] = append(envs["qa"], [2]string{name, id})
		case strings.Contains(lower, "pdn"):
			envs["pdn"] = append(envs["pdn"], [2]string{name, id})
		case strings.Contains(lower, "prod"):
			envs["prod"] = append(envs["prod"], [2]string{name, id})
		default:
			envs["other"] = append(envs["other"], [2]string{name, id})
		}
	}

	for _, env := range envOrder {
		list := envs[env]
		if len(list) == 0 {
			continue
		}
		sort.Slice(list, func(i, j int) bool { return list[i][0] < list[j][0] })
		fmt.Printf("  %s %s (%d):\n", pinTag, strings.ToUpper(env), len(list))
		for _, p := range list {
			fmt.Printf("    %s %s %s\n", dimStyle.Render("·"), p[0], dimStyle.Render("("+p[1]+")"))
		}
		fmt.Println()
	}

	if len(malformed) > 0 {
		fmt.Printf("  %s MALFORMED (%d):\n", warnStyle.Render("⚠"), len(malformed))
		for name, id := range malformed {
			fmt.Printf("    %s %s %s\n", warnStyle.Render("·"), name, dimStyle.Render("("+id+")"))
		}
		fmt.Println()
	}

	fmt.Printf("  Total: %s profiles\n", successStyle.Render(fmt.Sprintf("%d", len(profiles)+len(malformed))))
}

// ── Comando: ksw eks search-profiles ───────────────────

func handleSearchProfiles(term string) {
	fmt.Println(logoStyle.Render("⎈ ksw eks search-profiles"))
	fmt.Println()

	profiles, _, err := parseProfilesWithAccountID()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	lower := strings.ToLower(term)
	var matches [][2]string
	for name, id := range profiles {
		if strings.Contains(strings.ToLower(name), lower) || strings.Contains(id, term) {
			matches = append(matches, [2]string{name, id})
		}
	}

	if len(matches) == 0 {
		fmt.Printf("  %s No profiles matching '%s'\n", warnStyle.Render("✗"), term)
		fmt.Printf("  %s Try: ksw eks list-profiles\n", dimStyle.Render("·"))
		return
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i][0] < matches[j][0] })
	fmt.Printf("  %s %d profiles matching '%s':\n\n", successStyle.Render("✔"), len(matches), term)
	for i, m := range matches {
		fmt.Printf("  %2d. %s %s\n", i+1, m[0], dimStyle.Render("("+m[1]+")"))
	}
	fmt.Printf("\n  %s aws s3 ls --profile %s\n", dimStyle.Render("💡"), matches[0][0])
}

// ── Comando: ksw eks search-sso ────────────────────────

func handleSearchSSO(term string) {
	fmt.Println(logoStyle.Render("⎈ ksw eks search-sso"))
	fmt.Println()

	if err := checkAWSCLI(); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	fmt.Printf("  %s Fetching SSO accounts...\n", dimStyle.Render("⟳"))
	accounts, err := loadSSOAccounts()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	lower := strings.ToLower(term)
	var matches []ssoAccount
	for _, a := range accounts {
		if strings.Contains(strings.ToLower(a.AccountName), lower) || strings.Contains(a.AccountID, term) {
			matches = append(matches, a)
		}
	}

	if len(matches) == 0 {
		fmt.Printf("  %s No SSO accounts matching '%s'\n", warnStyle.Render("✗"), term)
		return
	}

	fmt.Printf("  %s %d SSO accounts matching '%s':\n\n", successStyle.Render("✔"), len(matches), term)
	for i, a := range matches {
		suggested := normalizeAccountName(a.AccountName)
		fmt.Printf("  %2d. %s %s\n", i+1, a.AccountName, dimStyle.Render("("+a.AccountID+")"))
		fmt.Printf("      %s profile: %s\n", dimStyle.Render("→"), suggested)
	}
}

// ── Comando: ksw eks add-profile ────────────────────────

func handleAddProfile(name, accountID, sessionName string) {
	fmt.Println(logoStyle.Render("⎈ ksw eks add-profile"))
	fmt.Println()

	if !isValidAccountID(accountID) {
		fmt.Fprintf(os.Stderr, "%s Invalid account ID '%s' (must be 12 digits)\n", warnStyle.Render("✗"), accountID)
		os.Exit(1)
	}

	ssoCfg := loadSSOSettingsFor(sessionName)

	if err := ensureSSOSessionFor(ssoCfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	if err := addProfileToConfigWith(name, accountID, ssoCfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	fmt.Printf("  %s Profile '%s' added (%s)\n", successStyle.Render("✔"), name, accountID)
	fmt.Printf("  %s session=%s  role=%s  region=%s\n", dimStyle.Render("·"), ssoCfg.SessionName, ssoCfg.RoleName, ssoCfg.Region)
}

// ── Detección de sesión SSO activa ────────────────────

type ssoActiveSession struct {
	SessionName string    // nombre de la sso-session (del campo "startUrl" o "ssoAccountId")
	StartURL    string    // start URL del token
	ExpiresAt   time.Time // cuándo expira el token
	IsValid     bool      // true si el token es válido y no expiró
	Region      string    // región del token si está disponible
}

// detectActiveSSOSessions examina ~/.aws/sso/cache y retorna las sesiones con tokens válidos.
// Soporta tanto el formato clásico (accessToken) como el nuevo de AWS CLI v2 (refreshToken).
func detectActiveSSOSessions() []ssoActiveSession {
	home, _ := os.UserHomeDir()
	cacheDir := filepath.Join(home, ".aws", "sso", "cache")

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil
	}

	// Primero leer las sesiones de ~/.aws/config para tener los nombres reales
	awsSessions := readAWSConfigSSOSessionDetails()

	seen := make(map[string]bool)
	var sessions []ssoActiveSession

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(cacheDir, e.Name()))
		if err != nil {
			continue
		}

		var cache map[string]interface{}
		if err := json.Unmarshal(data, &cache); err != nil {
			continue
		}

		// Buscar archivos con startUrl (registro de cliente o token de sesión)
		startURL, hasURL := cache["startUrl"].(string)
		if !hasURL || startURL == "" {
			continue
		}
		if seen[strings.TrimRight(startURL, "/#")] {
			continue
		}

		// Determinar si hay token válido: accessToken clásico O refreshToken nuevo
		hasToken := false
		if t, ok := cache["accessToken"].(string); ok && t != "" {
			hasToken = true
		}
		if t, ok := cache["refreshToken"].(string); ok && t != "" {
			hasToken = true
		}

		if !hasToken {
			continue
		}
		seen[strings.TrimRight(startURL, "/#")] = true

		var expiresAt time.Time
		isValid := false
		if expiresAtStr, ok := cache["expiresAt"].(string); ok && expiresAtStr != "" {
			for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05UTC", "2006-01-02T15:04:05Z", "2006-01-02T15:04:05.000Z"} {
				t, err := time.Parse(layout, expiresAtStr)
				if err == nil {
					expiresAt = t
					isValid = time.Now().Before(t)
					break
				}
			}
		}

		// Buscar nombre de sesión en aws config
		sessionName := ""
		region := ""
		for name, s := range awsSessions {
			if strings.TrimRight(s.startURL, "/#") == strings.TrimRight(startURL, "/#") {
				sessionName = name
				region = s.region
				break
			}
		}
		if sessionName == "" {
			sessionName = matchSessionNameByURL(startURL)
		}
		if r, ok := cache["region"].(string); ok && r != "" {
			region = r
		}

		sessions = append(sessions, ssoActiveSession{
			SessionName: sessionName,
			StartURL:    startURL,
			ExpiresAt:   expiresAt,
			IsValid:     isValid,
			Region:      region,
		})
	}

	// También agregar sesiones de aws config que NO tienen ningún archivo en cache con su startUrl
	for name, s := range awsSessions {
		alreadySeen := false
		for url := range seen {
			if strings.TrimRight(url, "/#") == strings.TrimRight(s.startURL, "/#") {
				alreadySeen = true
				break
			}
		}
		if alreadySeen {
			continue
		}
		sessions = append(sessions, ssoActiveSession{
			SessionName: name,
			StartURL:    s.startURL,
			IsValid:     false,
			Region:      s.region,
		})
	}

	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].IsValid != sessions[j].IsValid {
			return sessions[i].IsValid
		}
		return sessions[i].SessionName < sessions[j].SessionName
	})

	return sessions
}

type awsConfigSSOSession struct {
	startURL string
	region   string
}

// readAWSConfigSSOSessionDetails lee [sso-session ...] con su start_url y region de ~/.aws/config.
func readAWSConfigSSOSessionDetails() map[string]awsConfigSSOSession {
	f, err := os.Open(awsConfigPath())
	if err != nil {
		return nil
	}
	defer f.Close()

	result := make(map[string]awsConfigSSOSession)
	reHeader := regexp.MustCompile(`^\[sso-session\s+([^\]]+)\]$`)
	var cur string
	var cur_url, cur_region string

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if m := reHeader.FindStringSubmatch(line); m != nil {
			if cur != "" {
				result[cur] = awsConfigSSOSession{startURL: cur_url, region: cur_region}
			}
			cur = strings.TrimSpace(m[1])
			cur_url, cur_region = "", ""
			continue
		}
		if strings.HasPrefix(line, "[") {
			if cur != "" {
				result[cur] = awsConfigSSOSession{startURL: cur_url, region: cur_region}
			}
			cur = ""
			cur_url, cur_region = "", ""
			continue
		}
		if cur != "" && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			switch key {
			case "sso_start_url":
				cur_url = val
			case "sso_region":
				cur_region = val
			}
		}
	}
	if cur != "" {
		result[cur] = awsConfigSSOSession{startURL: cur_url, region: cur_region}
	}
	return result
}

// matchSessionNameByURL busca en ~/.ksw.json qué sesión SSO tiene ese start URL.
// readAWSConfigSSOSessions lee los nombres de [sso-session ...] de ~/.aws/config.
func readAWSConfigSSOSessions() []string {
	f, err := os.Open(awsConfigPath())
	if err != nil {
		return nil
	}
	defer f.Close()

	var sessions []string
	re := regexp.MustCompile(`^\[sso-session\s+([^\]]+)\]$`)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if m := re.FindStringSubmatch(line); m != nil {
			sessions = append(sessions, strings.TrimSpace(m[1]))
		}
	}
	return sessions
}

func matchSessionNameByURL(startURL string) string {
	cfg := loadConfig()
	for name, s := range cfg.SSOSessions {
		if strings.TrimRight(s.StartURL, "/#") == strings.TrimRight(startURL, "/#") {
			return name
		}
	}
	// Si no hay coincidencia, intentar extraer el nombre del URL
	// e.g. "https://d-9067080964.awsapps.com/start/#" -> "d-9067080964"
	parts := strings.Split(startURL, "/")
	for _, p := range parts {
		if strings.HasPrefix(p, "d-") || len(p) > 10 {
			if strings.Contains(p, ".awsapps.com") {
				sub := strings.Split(p, ".")[0]
				return sub
			}
		}
	}
	return startURL
}

// formatTimeRemaining retorna una string legible de cuánto tiempo falta para que expire.
func formatTimeRemaining(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	dur := time.Until(t)
	if dur < 0 {
		ago := -dur
		if ago < time.Hour {
			return fmt.Sprintf("expired %dm ago", int(ago.Minutes()))
		}
		return fmt.Sprintf("expired %dh ago", int(ago.Hours()))
	}
	if dur < time.Hour {
		return fmt.Sprintf("%dm remaining", int(dur.Minutes()))
	}
	h := int(dur.Hours())
	m := int(dur.Minutes()) % 60
	if m == 0 {
		return fmt.Sprintf("%dh remaining", h)
	}
	return fmt.Sprintf("%dh %dm remaining", h, m)
}

// ── TUI: ksw aws sso config ────────────────────────────

type ssoConfigStep int

const (
	ssoStepMenu       ssoConfigStep = iota // pantalla principal con sesión + opciones
	ssoStepPick                            // pick a session from list (edit/default/delete)
	ssoStepLoginPick                       // pick a session to login with
	ssoStepSyncPick                        // pick a session to use for sync
	ssoStepNewName                         // input name for new session
	ssoStepEdit                            // edit 4 fields
	ssoStepInfo                            // mensaje inline sin salir del TUI
	ssoStepLoggingIn                       // ejecutando aws sso login (ExecProcess)
	ssoStepPostLogin                       // login terminó, ofrecer sync u otras acciones
	ssoStepSyncing                         // spinner mientras se hace sync de profiles
	ssoStepSyncResult                      // resultado del sync de profiles
	ssoStepSyncingKube                     // spinner mientras se hace sync de kubeconfig
	ssoStepSyncKubeResult                  // resultado del sync de kubeconfig
	ssoStepConfirmDelete                   // confirmar eliminación de la única sesión
	ssoStepDone                            // confirmation (sale del TUI — solo para quit explícito)
)

type ssoMenuAction int

const (
	ssoActionLogin ssoMenuAction = iota
	ssoActionEdit
	ssoActionNew
	ssoActionSyncProfiles
	ssoActionSyncKubeconfig
	ssoActionDefault
	ssoActionDelete
	ssoActionQuit
)

var ssoMenuLabels = []string{
	"Login",
	"Edit existing session",
	"Create new session",
	"Sync all profiles",
	"Sync kubeconfig",
	"Set default session",
	"Delete session",
	"Quit",
}

type ssoConfigModel struct {
	step              ssoConfigStep
	menuAction        ssoMenuAction
	sessions          []string           // session names configured in ksw
	activeSessions    []ssoActiveSession // active SSO tokens detected in cache
	awsConfigSessions []string           // sso-session names from ~/.aws/config
	defaultN          string             // current default
	cursor            int
	input             string    // text input for new name
	fields            [4]string // session, url, region, role
	fieldIdx          int       // active field in edit mode (0-3)
	sessionKey        string    // key in sessions map
	setDefault        bool
	doneMsg           string // message to show on done (exits TUI)
	infoMsg           string // message to show inline (stays in TUI)
	loginSession      string // session name to login with
	loginErr          error  // error del último login intento
	profileCount      int    // profiles que referencian la sesión a eliminar
	syncResult           *syncProfilesResult
	kubeResult           *eksSyncResultData
	syncProgressLines    []syncProfileLine           // líneas en vivo durante sync profiles
	syncProfilesCh       <-chan syncProfileLine       // canal de progreso activo
	syncProfilesResultCh <-chan syncProfilesResult    // canal de resultado final
	syncTotal            int                         // total de profiles en el sync activo
	kubeProgressLines    []eksSyncLine               // líneas en vivo durante sync kubeconfig
	kubeProgressCh       <-chan eksSyncLine           // canal de progreso kubeconfig
	kubeResultCh         <-chan eksSyncResultData     // canal de resultado kubeconfig
	kubeTotal            int                         // total de profiles en el sync kube
	filterInput          string                       // búsqueda en ssoStepSyncPick
	spinnerFrame         int
	width             int
	height            int
	quitting          bool
}

var ssoFieldLabels = [4]string{
	"SSO Session Name",
	"SSO Start URL",
	"SSO Region",
	"SSO Role Name",
}

var ssoFieldHints = [4]string{
	"e.g. my-org",
	"e.g. https://d-XXXXXXXXXX.awsapps.com/start/#",
	"e.g. us-east-1",
	"e.g. MyOrgSysOpsRole",
}

// loginDoneMsg se envía cuando aws sso login termina (vía ExecProcess callback)
type loginDoneMsg struct{ err error }

// spinnerTickMsg se envía cada tick del spinner mientras syncing
type spinnerTickMsg struct{}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func spinnerTick() tea.Cmd {
	return tea.Tick(80*time.Millisecond, func(_ time.Time) tea.Msg {
		return spinnerTickMsg{}
	})
}

func startSyncProfiles(sessionName string) (<-chan syncProfileLine, <-chan syncProfilesResult, tea.Cmd) {
	progressCh := make(chan syncProfileLine, 512)
	resultCh := make(chan syncProfilesResult, 1)
	go func() {
		result := runSyncProfilesStreaming(sessionName, progressCh)
		close(progressCh)
		resultCh <- result
	}()
	return progressCh, resultCh, drainSyncProfilesCh(progressCh, resultCh)
}

func cmdRunSync(sessionName string) tea.Cmd {
	_, _, cmd := startSyncProfiles(sessionName)
	return cmd
}

// withSync arranca el sync y retorna el modelo actualizado + el cmd.
func withSync(m ssoConfigModel, sessionName string) (ssoConfigModel, tea.Cmd) {
	pCh, rCh, drainCmd := startSyncProfiles(sessionName)
	profiles, _ := parseAWSProfiles("")
	m.syncProfilesCh = pCh
	m.syncProfilesResultCh = rCh
	m.syncProgressLines = nil
	m.syncTotal = len(profiles)
	m.step = ssoStepSyncing
	m.spinnerFrame = 0
	return m, tea.Batch(spinnerTick(), drainCmd)
}

func cmdDoLogin(sessionName string) tea.Cmd {
	c := exec.Command("aws", "sso", "login", "--sso-session", sessionName)
	return tea.ExecProcess(c, func(err error) tea.Msg {
		return loginDoneMsg{err: err}
	})
}

type syncKubeMsgResult struct{ result eksSyncResultData }

// drainSyncKubeCh lee una línea de progreso o el resultado final del canal
func drainSyncKubeCh(progressCh <-chan eksSyncLine, resultCh <-chan eksSyncResultData) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-progressCh
		if !ok {
			return syncKubeMsgResult{result: <-resultCh}
		}
		return syncKubeProgressMsg{line: line}
	}
}

type syncKubeProgressMsg struct{ line eksSyncLine }

func startSyncKube() (<-chan eksSyncLine, <-chan eksSyncResultData, int, tea.Cmd) {
	profiles, _ := parseAWSProfiles("")
	progressCh := make(chan eksSyncLine, 256)
	resultCh := make(chan eksSyncResultData, 1)
	go func() {
		result := runEKSSyncTUIStreaming(profiles, progressCh)
		close(progressCh)
		resultCh <- result
	}()
	return progressCh, resultCh, len(profiles), drainSyncKubeCh(progressCh, resultCh)
}

func withSyncKube(m ssoConfigModel) (ssoConfigModel, tea.Cmd) {
	pCh, rCh, total, drainCmd := startSyncKube()
	m.kubeProgressCh = pCh
	m.kubeResultCh = rCh
	m.kubeProgressLines = nil
	m.kubeTotal = total
	m.step = ssoStepSyncingKube
	m.spinnerFrame = 0
	return m, tea.Batch(spinnerTick(), drainCmd)
}

func (m ssoConfigModel) Init() tea.Cmd {
	return tea.WindowSize()
}

func (m ssoConfigModel) isMenuStep() bool {
	return m.step == ssoStepMenu || m.step == ssoStepPick || m.step == ssoStepLoginPick || m.step == ssoStepSyncPick
}

func (m ssoConfigModel) isStatusStep() bool {
	return false // removed, status is now embedded in menu
}

func (m ssoConfigModel) isInputStep() bool {
	return m.step == ssoStepNewName
}

func (m ssoConfigModel) isEditStep() bool {
	return m.step == ssoStepEdit
}

func (m ssoConfigModel) filteredSessions() []string {
	if m.filterInput == "" {
		return m.sessions
	}
	term := strings.ToLower(m.filterInput)
	var out []string
	for _, s := range m.sessions {
		if strings.Contains(strings.ToLower(s), term) {
			out = append(out, s)
		}
	}
	return out
}

func (m ssoConfigModel) menuLen() int {
	switch m.step {
	case ssoStepMenu:
		return len(ssoMenuLabels)
	case ssoStepPick:
		return len(m.sessions)
	case ssoStepLoginPick:
		return len(m.awsConfigSessions)
	case ssoStepSyncPick:
		return len(m.filteredSessions())
	}
	return 0
}

func (m ssoConfigModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case loginDoneMsg:
		m.loginErr = msg.err
		m.step = ssoStepPostLogin
		// Recargar sesiones activas tras el login
		m.activeSessions = detectActiveSSOSessions()
		return m, nil

	case spinnerTickMsg:
		if m.step == ssoStepSyncing || m.step == ssoStepSyncingKube {
			m.spinnerFrame = (m.spinnerFrame + 1) % len(spinnerFrames)
			return m, spinnerTick()
		}
		return m, nil

	case syncProfilesProgressMsg:
		m.syncProgressLines = append(m.syncProgressLines, msg.line)
		return m, drainSyncProfilesCh(m.syncProfilesCh, m.syncProfilesResultCh)

	case syncProfilesMsgResult:
		m.syncResult = &msg.result
		m.syncProgressLines = nil
		m.syncProfilesCh = nil
		m.syncProfilesResultCh = nil
		m.step = ssoStepSyncResult
		return m, nil

	case syncKubeProgressMsg:
		m.kubeProgressLines = append(m.kubeProgressLines, msg.line)
		return m, drainSyncKubeCh(m.kubeProgressCh, m.kubeResultCh)

	case syncKubeMsgResult:
		m.kubeResult = &msg.result
		m.kubeProgressLines = nil
		m.kubeProgressCh = nil
		m.kubeResultCh = nil
		m.step = ssoStepSyncKubeResult
		return m, nil

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			// Ctrl+C siempre sale
			m.quitting = true
			return m, tea.Quit

		case tea.KeyEsc:
			if m.step == ssoStepSyncing || m.step == ssoStepSyncingKube {
				return m, nil // Esc no cancela durante sync
			}
			if m.step == ssoStepPick || m.step == ssoStepLoginPick || m.step == ssoStepSyncPick ||
				m.step == ssoStepNewName || m.step == ssoStepEdit || m.step == ssoStepInfo ||
				m.step == ssoStepSyncResult || m.step == ssoStepSyncKubeResult ||
				m.step == ssoStepPostLogin || m.step == ssoStepConfirmDelete {
				m.step = ssoStepMenu
				m.cursor = 0
				m.infoMsg = ""
				m.filterInput = ""
				m.syncResult = nil
				m.kubeResult = nil
				m.loginErr = nil
				return m, nil
			}
			m.quitting = true
			return m, tea.Quit

		default:
			if m.step == ssoStepSyncing || m.step == ssoStepSyncingKube {
				return m, nil // bloquear otros inputs durante sync
			}
		}
		if m.step == ssoStepSyncing || m.step == ssoStepSyncingKube {
			return m, nil
		}

		switch msg.Type {
		case tea.KeyUp:
			if m.isMenuStep() && m.cursor > 0 {
				m.cursor--
			}
			if m.isEditStep() && m.fieldIdx > 0 {
				m.fieldIdx--
			}
			return m, nil

		case tea.KeyDown:
			if m.isMenuStep() {
				max := m.menuLen() - 1
				if m.cursor < max {
					m.cursor++
				}
			}
			if m.isEditStep() && m.fieldIdx < 3 {
				m.fieldIdx++
			}
			return m, nil

		case tea.KeyTab:
			if m.isEditStep() {
				m.fieldIdx = (m.fieldIdx + 1) % 4
			}
			return m, nil

		case tea.KeyEnter:
			return m.handleSSOEnter()

		case tea.KeyBackspace:
			if m.step == ssoStepSyncPick && len(m.filterInput) > 0 {
				r := []rune(m.filterInput)
				m.filterInput = string(r[:len(r)-1])
				m.cursor = 0
			}
			if m.isInputStep() && len(m.input) > 0 {
				r := []rune(m.input)
				m.input = string(r[:len(r)-1])
			}
			if m.isEditStep() {
				f := m.fields[m.fieldIdx]
				if len(f) > 0 {
					r := []rune(f)
					m.fields[m.fieldIdx] = string(r[:len(r)-1])
				}
			}
			return m, nil

		case tea.KeyRunes:
			// Filtro en ssoStepSyncPick
			if m.step == ssoStepSyncPick {
				m.filterInput += string(msg.Runes)
				m.cursor = 0
				return m, nil
			}
			// Tecla 'y' desde confirm-delete → eliminar sesión + profiles
			if m.step == ssoStepConfirmDelete && string(msg.Runes) == "y" {
				return m.execDeleteSession(true), nil
			}
			// Tecla 'n' desde confirm-delete → eliminar solo la sesión, conservar profiles
			if m.step == ssoStepConfirmDelete && string(msg.Runes) == "n" {
				return m.execDeleteSession(false), nil
			}
		// Tecla 's' desde session-saved info → lanzar sync (sin login previo)
		if m.step == ssoStepInfo && strings.HasPrefix(m.infoMsg, "session-saved:") && string(msg.Runes) == "s" {
			syncSession := strings.TrimPrefix(m.infoMsg, "session-saved:")
			m.infoMsg = ""
			nm, cmd := withSync(m, syncSession)
			return nm, cmd
		}
		// Tecla 'k' desde sync-result → lanzar sync kubeconfig
		if m.step == ssoStepSyncResult && string(msg.Runes) == "k" {
			return withSyncKube(m)
		}
		// Tecla 's' desde post-login → lanzar sync
		if m.step == ssoStepPostLogin && string(msg.Runes) == "s" && m.loginErr == nil {
			syncSession := m.loginSession
			if syncSession == "" {
				syncSession = m.defaultN
			}
			m.loginErr = nil
			nm, cmd := withSync(m, syncSession)
			return nm, cmd
		}
			if m.isInputStep() {
				m.input += string(msg.Runes)
			}
			if m.isEditStep() {
				m.fields[m.fieldIdx] += string(msg.Runes)
			}
			return m, nil

		case tea.KeySpace:
			if m.isInputStep() {
				m.input += " "
			}
			if m.isEditStep() {
				m.fields[m.fieldIdx] += " "
			}
			return m, nil
		}
	}
	return m, nil
}

// execDeleteSession elimina la sesión de ksw y de ~/.aws/config.
// Si withProfiles=true también elimina los [profile] que la referencian.
func (m ssoConfigModel) execDeleteSession(withProfiles bool) ssoConfigModel {
	cfg := loadConfig()
	delete(cfg.SSOSessions, m.sessionKey)
	if cfg.SSODefault == m.sessionKey {
		cfg.SSODefault = ""
		for n := range cfg.SSOSessions {
			cfg.SSODefault = n
			break
		}
	}
	_ = saveConfig(cfg)

	if withProfiles {
		_ = removeSSOSessionFromConfig(m.sessionKey) // borra sesión + profiles
	} else {
		// Solo borrar el bloque [sso-session], dejar profiles intactos
		_ = removeSSOSessionBlockOnly(m.sessionKey)
	}

	newSessions, newDefault := listSSOSessions()
	m.sessions = newSessions
	m.defaultN = newDefault
	m.activeSessions = detectActiveSSOSessions()
	m.infoMsg = "session-deleted:" + m.sessionKey
	m.step = ssoStepInfo
	return m
}

func (m ssoConfigModel) handleSSOEnter() (tea.Model, tea.Cmd) {
	switch m.step {
	case ssoStepMenu:
		action := ssoMenuAction(m.cursor)
		m.menuAction = action
		switch action {
		case ssoActionLogin:
			// Si hay un default session configurado en ksw, usar ese
			if m.defaultN != "" {
				m.loginSession = m.defaultN
				m.step = ssoStepLoggingIn
				return m, cmdDoLogin(m.defaultN)
			}
			// Si hay sesiones en activeSessions con nombre, usar la primera
			for _, s := range m.activeSessions {
				if s.SessionName != "" {
					m.loginSession = s.SessionName
					m.step = ssoStepLoggingIn
					return m, cmdDoLogin(s.SessionName)
				}
			}
			// Leer sso-sessions de ~/.aws/config y ofrecer selección
			if len(m.awsConfigSessions) == 1 {
				m.loginSession = m.awsConfigSessions[0]
				m.step = ssoStepLoggingIn
				return m, cmdDoLogin(m.awsConfigSessions[0])
			}
			if len(m.awsConfigSessions) > 1 {
				m.step = ssoStepLoginPick
				m.cursor = 0
				return m, nil
			}
			// No hay ninguna sesión configurada — mostrar info y ofrecer crear
			m.infoMsg = "no-sessions"
			m.step = ssoStepInfo
			return m, nil

		case ssoActionEdit:
			if len(m.sessions) == 0 {
				// Sin sesiones en ksw: mostrar info para crear primero
				m.infoMsg = "no-sessions"
				m.step = ssoStepInfo
				return m, nil
			}
			if len(m.sessions) == 1 {
				m.sessionKey = m.sessions[0]
				m.loadSessionFields()
				m.step = ssoStepEdit
				m.fieldIdx = 0
			} else {
				m.step = ssoStepPick
				m.cursor = 0
			}
		case ssoActionNew:
			m.step = ssoStepNewName
			m.input = ""
			m.setDefault = len(m.sessions) == 0
		case ssoActionSyncProfiles:
			if len(m.sessions) == 0 {
				m.infoMsg = "no-sessions"
				m.step = ssoStepInfo
				return m, nil
			}
		if len(m.sessions) == 1 {
			// Solo una sesión, sincronizar directamente
			return withSync(m, m.sessions[0])
		}
			// Múltiples sesiones: preguntar cuál usar
			m.step = ssoStepSyncPick
			m.cursor = 0
			m.filterInput = ""
			// Pre-seleccionar la default si existe
			for i, s := range m.sessions {
				if s == m.defaultN {
					m.cursor = i
					break
				}
			}
			return m, nil
		case ssoActionSyncKubeconfig:
			return withSyncKube(m)

		case ssoActionDefault:
			if len(m.sessions) == 0 {
				m.infoMsg = "no-sessions"
				m.step = ssoStepInfo
				return m, nil
			}
			if len(m.sessions) == 1 {
				cfg := loadConfig()
				cfg.SSODefault = m.sessions[0]
				_ = saveConfig(cfg)
				m.defaultN = m.sessions[0]
				m.infoMsg = "default-set:" + m.sessions[0]
				m.step = ssoStepInfo
				return m, nil
			}
			m.step = ssoStepPick
			m.cursor = 0
		case ssoActionDelete:
			if len(m.sessions) == 0 {
				m.infoMsg = "no-sessions"
				m.step = ssoStepInfo
				return m, nil
			}
			if len(m.sessions) == 1 {
				m.sessionKey = m.sessions[0]
				m.profileCount = countProfilesForSession(m.sessionKey)
				m.step = ssoStepConfirmDelete
				return m, nil
			}
			m.step = ssoStepPick
			m.cursor = 0
		case ssoActionQuit:
			m.quitting = true
			return m, tea.Quit
		}
		return m, nil

	case ssoStepInfo:
		switch {
		case m.infoMsg == "no-sessions":
			// Enter → crear nueva sesión
			m.step = ssoStepNewName
			m.input = ""
			m.setDefault = true
			m.infoMsg = ""
		case strings.HasPrefix(m.infoMsg, "session-saved:"):
			// Enter → hacer login con la sesión recién guardada
			sessionName := strings.TrimPrefix(m.infoMsg, "session-saved:")
			m.loginSession = sessionName
			m.infoMsg = ""
			m.step = ssoStepLoggingIn
			return m, cmdDoLogin(sessionName)
		case strings.HasPrefix(m.infoMsg, "session-deleted:"):
			// Enter → volver al menú
			m.step = ssoStepMenu
			m.cursor = 0
			m.infoMsg = ""
			return m, nil
		default:
			// Cualquier otro mensaje → volver al menú
			m.step = ssoStepMenu
			m.infoMsg = ""
		}
		return m, nil

	case ssoStepLoginPick:
		selected := m.awsConfigSessions[m.cursor]
		m.loginSession = selected
		m.step = ssoStepLoggingIn
		return m, cmdDoLogin(selected)

	case ssoStepPostLogin:
		if m.loginErr != nil {
			// Login falló → volver al menú
			m.step = ssoStepMenu
			m.loginErr = nil
			return m, nil
		}
		// Login exitoso, Enter → lanzar sync
		syncSession := m.loginSession
		if syncSession == "" {
			syncSession = m.defaultN
		}
		m.loginErr = nil
		return withSync(m, syncSession)

	case ssoStepSyncResult:
		// Enter → volver al menú
		m.step = ssoStepMenu
		m.cursor = 0
		m.syncResult = nil
		return m, nil

	case ssoStepSyncKubeResult:
		// Enter → volver al menú
		m.step = ssoStepMenu
		m.cursor = 0
		m.kubeResult = nil
		return m, nil

	case ssoStepConfirmDelete:
		// Enter con 'y' o la tecla s se maneja en KeyRunes; aquí Enter sin input = cancelar
		// (la confirmación real se hace via KeyRunes 'y')
		m.step = ssoStepMenu
		m.cursor = 0
		return m, nil

	case ssoStepSyncPick:
		filtered := m.filteredSessions()
		if len(filtered) == 0 {
			return m, nil
		}
		selected := filtered[m.cursor]
		m.filterInput = ""
		return withSync(m, selected)

	case ssoStepPick:
		selected := m.sessions[m.cursor]
		switch m.menuAction {
		case ssoActionEdit:
			m.sessionKey = selected
			m.loadSessionFields()
			m.step = ssoStepEdit
			m.fieldIdx = 0
		case ssoActionDefault:
			cfg := loadConfig()
			cfg.SSODefault = selected
			_ = saveConfig(cfg)
			m.defaultN = selected
			m.infoMsg = "default-set:" + selected
			m.step = ssoStepInfo
			return m, nil
		case ssoActionDelete:
			m.sessionKey = selected
			m.profileCount = countProfilesForSession(selected)
			m.step = ssoStepConfirmDelete
			return m, nil
		}
		return m, nil

	case ssoStepNewName:
		name := strings.TrimSpace(m.input)
		if name == "" {
			return m, nil
		}
		m.sessionKey = name
		m.setDefault = len(m.sessions) == 0
		// Para sesión nueva: campos vacíos excepto el nombre y región por defecto
		// Solo pre-rellenar si ya existe en ksw (edición de existente)
		cfg := loadConfig()
		if existing, ok := cfg.SSOSessions[name]; ok {
			m.fields = [4]string{existing.SessionName, existing.StartURL, existing.Region, existing.RoleName}
		} else {
			// Nueva sesión: solo el nombre, resto vacío
			m.fields = [4]string{name, "", "us-east-1", ""}
		}
		m.step = ssoStepEdit
		m.fieldIdx = 0 // empezar desde el nombre para que usuario pueda confirmar
		return m, nil

	case ssoStepEdit:
		s := ssoConfig{
			SessionName: strings.TrimSpace(m.fields[0]),
			StartURL:    strings.TrimSpace(m.fields[1]),
			Region:      strings.TrimSpace(m.fields[2]),
			RoleName:    strings.TrimSpace(m.fields[3]),
		}
		if s.SessionName == "" {
			s.SessionName = m.sessionKey
		}
		if s.Region == "" {
			s.Region = "us-east-1"
		}
		key := m.sessionKey
		if key == "" {
			key = s.SessionName
		}
		// Guardar en ~/.ksw.json
		if err := saveSSOSession(key, s, m.setDefault); err != nil {
			m.doneMsg = "✗ Error saving config: " + err.Error()
			m.step = ssoStepDone
			return m, tea.Quit
		}
		// Escribir también en ~/.aws/config (sso-session + ensureSSOSession)
		if err := upsertSSOSessionInConfig(s); err != nil {
			m.doneMsg = "✗ Error updating ~/.aws/config: " + err.Error()
			m.step = ssoStepDone
			return m, tea.Quit
		}
		// Recargar sesiones y mostrar confirmación inline (sin salir del TUI)
		newSessions, newDefault := listSSOSessions()
		m.sessions = newSessions
		m.defaultN = newDefault
		m.infoMsg = "session-saved:" + key
		m.step = ssoStepInfo
		return m, nil
	}
	return m, nil
}

func (m *ssoConfigModel) loadSessionFields() {
	cfg := loadConfig()
	if s, ok := cfg.SSOSessions[m.sessionKey]; ok {
		m.fields = [4]string{s.SessionName, s.StartURL, s.Region, s.RoleName}
	} else {
		// Sesión no existe en ksw aún, intentar desde aws config
		awsSessions := readAWSConfigSSOSessionDetails()
		if s, ok := awsSessions[m.sessionKey]; ok {
			m.fields = [4]string{m.sessionKey, s.startURL, s.region, ""}
		} else {
			m.fields = [4]string{m.sessionKey, "", "us-east-1", ""}
		}
	}
}

func (m ssoConfigModel) View() string {
	if m.quitting || m.width == 0 {
		return ""
	}

	titleSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
	dim := lipgloss.NewStyle().Foreground(lipgloss.Color("#555"))
	sel := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00d4ff"))
	label := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#bd93f9"))
	inputSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b")).Bold(true)
	textSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2"))
	barSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#333"))
	okSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b"))
	normalSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#999"))
	warnSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#ff5555"))
	activeSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b"))
	expiredSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))
	urlSt := lipgloss.NewStyle().Foreground(lipgloss.Color("#666"))

	w := m.width
	if w < 20 {
		w = 60
	}
	innerW := w - 4
	if innerW < 20 {
		innerW = 56
	}

	header := "  " + titleSt.Render("⎈ ksw aws sso config")
	if isLicenseValid() {
		premiumSt := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f4a261"))
		header += "  " + premiumSt.Render("★ premium")
	}
	topBar := "  " + barSt.Render(strings.Repeat("─", innerW))

	var lines []string

	switch m.step {
	case ssoStepMenu:
		// ── Header: sesión activa ──
		// Filtrar: solo mostrar sesiones que existen en ksw (m.sessions)
		var relevantSessions []ssoActiveSession
		for _, as := range m.activeSessions {
			for _, name := range m.sessions {
				if as.SessionName == name {
					relevantSessions = append(relevantSessions, as)
					break
				}
			}
		}
		if len(m.sessions) > 0 && len(relevantSessions) == 0 {
			// Hay sesiones configuradas pero ninguna tiene token activo
			relevantSessions = []ssoActiveSession{{
				SessionName: m.defaultN,
				IsValid:     false,
			}}
			if relevantSessions[0].SessionName == "" && len(m.sessions) > 0 {
				relevantSessions[0].SessionName = m.sessions[0]
			}
		}
		if len(relevantSessions) > 0 {
			s := relevantSessions[0] // la primera es la más relevante (válida primero)
			var statusMark, statusStr string
			if s.IsValid {
				statusMark = activeSt.Render("●")
				statusStr = activeSt.Render(formatTimeRemaining(s.ExpiresAt))
			} else {
				statusMark = expiredSt.Render("○")
				statusStr = expiredSt.Render(formatTimeRemaining(s.ExpiresAt))
			}
			name := textSt.Render(s.SessionName)
			if s.SessionName == m.defaultN {
				name += "  " + okSt.Render("(default)")
			}
			lines = append(lines, "  "+statusMark+"  "+label.Render("Session")+"  "+name+"  "+statusStr)
			lines = append(lines, "     "+urlSt.Render(s.StartURL))
			if !s.ExpiresAt.IsZero() {
				expFmt := s.ExpiresAt.Local().Format("2006-01-02 15:04")
				lines = append(lines, "     "+dim.Render("expires "+expFmt))
			}
		} else {
			lines = append(lines, "  "+expiredSt.Render("○")+"  "+label.Render("Session")+"  "+dim.Render("no session configured"))
		}
		lines = append(lines, "")
		lines = append(lines, "  "+barSt.Render(strings.Repeat("─", innerW-2)))
		lines = append(lines, "")

		// ── Opciones ──
		lines = append(lines, "  "+label.Render("Options")+"  "+dim.Render("↑↓ navigate · enter select · esc quit"))
		lines = append(lines, "")
		for i, l := range ssoMenuLabels {
			if i == m.cursor {
				lines = append(lines, "  "+sel.Render("❯ "+l))
			} else {
				lines = append(lines, "    "+normalSt.Render(l))
			}
		}

	case ssoStepLoginPick:
		lines = append(lines, "  "+label.Render("Select session to login")+"  "+dim.Render("↑↓ navigate · enter select · esc back"))
		lines = append(lines, "")
		for i, s := range m.awsConfigSessions {
			if i == m.cursor {
				lines = append(lines, "  "+sel.Render("❯ "+s))
			} else {
				lines = append(lines, "    "+normalSt.Render(s))
			}
		}

	case ssoStepPick:
		action := "Select session"
		switch m.menuAction {
		case ssoActionEdit:
			action = "Select session to edit"
		case ssoActionDefault:
			action = "Select new default"
		case ssoActionDelete:
			action = "Select session to delete"
		}
		lines = append(lines, "  "+label.Render(action)+"  "+dim.Render("↑↓ navigate · enter select · esc back"))
		lines = append(lines, "")
		for i, s := range m.sessions {
			marker := ""
			if s == m.defaultN {
				marker = " " + dim.Render("(default)")
			}
			if i == m.cursor {
				lines = append(lines, "  "+sel.Render("❯ "+s)+marker)
			} else {
				lines = append(lines, "    "+normalSt.Render(s)+marker)
			}
		}

	case ssoStepSyncPick:
		filtered := m.filteredSessions()
		cursorBlink := dim.Render("▎")
		if m.filterInput == "" {
			lines = append(lines, "  "+inputSt.Render("/ ")+dim.Render("type to search...")+cursorBlink)
		} else {
			lines = append(lines, "  "+inputSt.Render("/ ")+sel.Render(m.filterInput)+cursorBlink)
		}
		lines = append(lines, "")
		total := fmt.Sprintf("%d of %d", len(filtered), len(m.sessions))
		lines = append(lines, "  "+dim.Render(total+"  ↑↓ navigate · enter select · esc back"))
		lines = append(lines, "")

		if len(filtered) == 0 {
			lines = append(lines, "  "+dim.Render("no sessions match '"+m.filterInput+"'"))
		} else {
			visibleRows := m.height - 7
			if visibleRows < 5 {
				visibleRows = 5
			}
			start := 0
			if m.cursor >= visibleRows {
				start = m.cursor - visibleRows + 1
			}
			end := start + visibleRows
			if end > len(filtered) {
				end = len(filtered)
			}
			if start > 0 {
				lines = append(lines, "  "+dim.Render(fmt.Sprintf("↑ %d more above", start)))
			}
			for i := start; i < end; i++ {
				s := filtered[i]
				marker := ""
				if s == m.defaultN {
					marker = dim.Render("  (default)")
				}
				if i == m.cursor {
					lines = append(lines, "  "+sel.Render("❯ "+s)+marker)
				} else {
					lines = append(lines, "    "+normalSt.Render(s)+marker)
				}
			}
			if end < len(filtered) {
				lines = append(lines, "  "+dim.Render(fmt.Sprintf("↓ %d more below", len(filtered)-end)))
			}
		}

	case ssoStepNewName:
		lines = append(lines, "  "+label.Render("New session name")+"  "+dim.Render("enter to confirm · esc back"))
		lines = append(lines, "")
		lines = append(lines, "  "+inputSt.Render("› ")+textSt.Render(m.input)+dim.Render("▎"))

	case ssoStepEdit:
		lines = append(lines, "  "+label.Render("Configure: "+m.sessionKey)+"  "+
			dim.Render("↑↓/tab navigate · enter save · esc back"))
		lines = append(lines, "")

		for i, l := range ssoFieldLabels {
			pointer := "  "
			style := normalSt
			if i == m.fieldIdx {
				pointer = sel.Render("❯ ")
				style = textSt
			}

			val := m.fields[i]
			cursor := ""
			if i == m.fieldIdx {
				cursor = dim.Render("▎")
			}

			if val == "" && i != m.fieldIdx {
				lines = append(lines, "  "+pointer+label.Render(l)+"  "+dim.Render(ssoFieldHints[i]))
			} else {
				lines = append(lines, "  "+pointer+label.Render(l))
				lines = append(lines, "    "+inputSt.Render("› ")+style.Render(val)+cursor)
			}
			lines = append(lines, "")
		}

	case ssoStepInfo:
		switch {
		case m.infoMsg == "no-sessions":
			lines = append(lines, "  "+warnSt.Render("✗ No SSO sessions configured in ksw"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("You need to create a session first."))
			lines = append(lines, "")
			lines = append(lines, "  "+okSt.Render("→ press enter to create one now"))
			lines = append(lines, "  "+dim.Render("  press esc to go back"))
		// cannot-delete-only is no longer used (replaced by ssoStepConfirmDelete)
		case strings.HasPrefix(m.infoMsg, "session-saved:"):
			sessionName := strings.TrimPrefix(m.infoMsg, "session-saved:")
			lines = append(lines, "  "+okSt.Render("✔ Session '"+sessionName+"' saved"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("Written to ~/.ksw.json and ~/.aws/config"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("Next steps:"))
			lines = append(lines, "  "+okSt.Render("→ press enter")+" "+dim.Render("to login now  (recommended)"))
			lines = append(lines, "  "+okSt.Render("→ press s    ")+" "+dim.Render("to sync profiles without login"))
			lines = append(lines, "  "+dim.Render("  press esc   to go back to menu"))
		case strings.HasPrefix(m.infoMsg, "session-deleted:"):
			sessionName := strings.TrimPrefix(m.infoMsg, "session-deleted:")
			lines = append(lines, "  "+okSt.Render("✔ Session '"+sessionName+"' deleted"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("press esc to go back to menu"))
		case strings.HasPrefix(m.infoMsg, "default-set:"):
			name := strings.TrimPrefix(m.infoMsg, "default-set:")
			lines = append(lines, "  "+okSt.Render("✔ Default session set to '"+name+"'"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("press esc to go back"))
		default:
			lines = append(lines, "  "+dim.Render(m.infoMsg))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("press esc to go back"))
		}

	case ssoStepConfirmDelete:
		lines = append(lines, "  "+warnSt.Render("⚠  Delete session '"+m.sessionKey+"'?"))
		lines = append(lines, "")
		if m.profileCount > 0 {
			lines = append(lines, "  "+dim.Render(fmt.Sprintf("Found %d profile(s) in ~/.aws/config using this session.", m.profileCount)))
			lines = append(lines, "  "+dim.Render("Without the session, those profiles will stop working."))
			lines = append(lines, "")
			lines = append(lines, "  "+warnSt.Render("→ press y")+" "+dim.Render(fmt.Sprintf("  delete session + %d profile(s)  (recommended)", m.profileCount)))
			lines = append(lines, "  "+okSt.Render("→ press n")+" "+dim.Render("  delete session only, keep profiles"))
		} else {
			lines = append(lines, "  "+dim.Render("No profiles reference this session in ~/.aws/config."))
			lines = append(lines, "")
			lines = append(lines, "  "+warnSt.Render("→ press y")+" "+dim.Render("  confirm delete"))
		}
		lines = append(lines, "  "+dim.Render("  press esc   cancel"))

	case ssoStepLoggingIn:
		lines = append(lines, "  "+dim.Render("Running aws sso login --sso-session "+m.loginSession+"..."))
		lines = append(lines, "")
		lines = append(lines, "  "+dim.Render("(browser may open — follow the prompts)"))

	case ssoStepPostLogin:
		if m.loginErr != nil {
			lines = append(lines, "  "+warnSt.Render("✗ Login failed: "+m.loginErr.Error()))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("press esc to go back to menu"))
		} else {
			lines = append(lines, "  "+okSt.Render("✔ Logged in to '"+m.loginSession+"'"))
			lines = append(lines, "")
			lines = append(lines, "  "+okSt.Render("→ press enter to sync all profiles now"))
			lines = append(lines, "  "+dim.Render("  press esc  to go back to menu"))
		}

	case ssoStepSyncing:
		spinner := spinnerFrames[m.spinnerFrame]
		lines = append(lines, "  "+okSt.Render(spinner)+" "+dim.Render("Fetching SSO accounts and syncing profiles..."))
		lines = append(lines, "")
		maxLines := m.height - 8
		if maxLines < 4 {
			maxLines = 4
		}
		shown := m.syncProgressLines
		if len(shown) > maxLines {
			shown = shown[len(shown)-maxLines:]
		}
		for _, l := range shown {
			if l.ok {
				lines = append(lines, "    "+okSt.Render("✔")+" "+dim.Render(l.text))
			} else {
				lines = append(lines, "    "+warnSt.Render("✗")+" "+dim.Render(l.text))
			}
		}
		if len(m.syncProgressLines) == 0 {
			lines = append(lines, "  "+dim.Render("Contacting AWS SSO..."))
		} else {
			processed := len(m.syncProgressLines)
			bar := tuiProgressBar(processed, m.syncTotal, 24)
			lines = append(lines, "")
			lines = append(lines, fmt.Sprintf("  %s  %s",
				bar,
				dim.Render(fmt.Sprintf("%d / %d  ·  ctrl+c to cancel", processed, m.syncTotal))))
		}

	case ssoStepSyncingKube:
		spinner := spinnerFrames[m.spinnerFrame]
		lines = append(lines, "  "+okSt.Render(spinner)+" "+dim.Render("Syncing kubeconfig with AWS clusters..."))
		lines = append(lines, "")
		maxLines := m.height - 8
		if maxLines < 4 {
			maxLines = 4
		}
		shown := m.kubeProgressLines
		if len(shown) > maxLines {
			shown = shown[len(shown)-maxLines:]
		}
		for _, l := range shown {
			if l.ok {
				lines = append(lines, "    "+okSt.Render("✔")+" "+dim.Render(l.msg))
			} else {
				lines = append(lines, "    "+warnSt.Render("✗")+" "+dim.Render(l.msg))
			}
		}
		if len(m.kubeProgressLines) == 0 {
			lines = append(lines, "  "+dim.Render("Querying AWS..."))
		} else {
			processed := len(m.kubeProgressLines)
			bar := tuiProgressBar(processed, m.kubeTotal, 24)
			lines = append(lines, "")
			lines = append(lines, fmt.Sprintf("  %s  %s",
				bar,
				dim.Render(fmt.Sprintf("%d / %d  ·  ctrl+c to cancel", processed, m.kubeTotal))))
		}

	case ssoStepSyncKubeResult:
		if m.kubeResult == nil {
			lines = append(lines, "  "+dim.Render("No result"))
		} else if m.kubeResult.err != nil {
			lines = append(lines, "  "+warnSt.Render("✗ Kubeconfig sync failed"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render(m.kubeResult.err.Error()))
		} else {
			lines = append(lines, "  "+okSt.Render(fmt.Sprintf("✔ Kubeconfig sync — %d added, %d skipped, %d failed",
				m.kubeResult.added, m.kubeResult.skipped, m.kubeResult.failed)))
			lines = append(lines, "")
			shown := m.kubeResult.lines
			if len(shown) > 12 {
				shown = shown[:12]
			}
			for _, l := range shown {
				if l.ok {
					lines = append(lines, "    "+okSt.Render("✔")+" "+dim.Render(l.msg))
				} else {
					lines = append(lines, "    "+warnSt.Render("✗")+" "+dim.Render(l.msg))
				}
			}
			if len(m.kubeResult.lines) > 12 {
				lines = append(lines, "    "+dim.Render(fmt.Sprintf("... and %d more", len(m.kubeResult.lines)-12)))
			}
		}
		lines = append(lines, "")
		lines = append(lines, "  "+dim.Render("press enter or esc to go back to menu"))

	case ssoStepSyncResult:
		if m.syncResult == nil {
			lines = append(lines, "  "+dim.Render("No result"))
		} else if m.syncResult.err != nil {
			lines = append(lines, "  "+warnSt.Render("✗ Sync failed"))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render(m.syncResult.err.Error()))
			lines = append(lines, "")
			lines = append(lines, "  "+dim.Render("Make sure you are logged in first (Login option)."))
		} else {
			if m.syncResult.added == 0 && m.syncResult.fixed == 0 {
				lines = append(lines, "  "+okSt.Render("✔ All profiles are up to date"))
			} else {
				lines = append(lines, "  "+okSt.Render(fmt.Sprintf("✔ Sync complete — %d added, %d fixed, %d total profiles",
					m.syncResult.added, m.syncResult.fixed, m.syncResult.total)))
			}
			lines = append(lines, "")
			// Mostrar hasta 12 líneas de detalle
			shown := m.syncResult.lines
			if len(shown) > 12 {
				shown = shown[:12]
			}
			for _, l := range shown {
				if l.ok {
					lines = append(lines, "    "+okSt.Render("✔")+" "+dim.Render(l.text))
				} else {
					lines = append(lines, "    "+warnSt.Render("✗")+" "+dim.Render(l.text))
				}
			}
			if len(m.syncResult.lines) > 12 {
				lines = append(lines, "    "+dim.Render(fmt.Sprintf("... and %d more", len(m.syncResult.lines)-12)))
			}
		}
		lines = append(lines, "")
		if m.syncResult != nil && m.syncResult.err == nil {
			lines = append(lines, "  "+okSt.Render("→ [k] Sync kubeconfig now"))
		}
		lines = append(lines, "  "+dim.Render("  [enter / esc] back to menu"))

	case ssoStepDone:
		if strings.HasPrefix(m.doneMsg, "✔") {
			lines = append(lines, "  "+okSt.Render(m.doneMsg))
		} else if m.doneMsg != "" {
			lines = append(lines, "  "+warnSt.Render(m.doneMsg))
		}
	}

	// Pad
	availH := m.height - 4
	if availH < 3 {
		availH = 3
	}
	for len(lines) < availH {
		lines = append(lines, "")
	}

	bottomBar := "  " + barSt.Render(strings.Repeat("─", innerW))

	var b strings.Builder
	b.WriteString(header + "\n")
	b.WriteString(topBar + "\n")
	for _, l := range lines {
		b.WriteString(l + "\n")
	}
	b.WriteString(bottomBar)
	return b.String()
}

func handleSSOLogin() {
	sessions, defaultName := listSSOSessions()
	sessionName := defaultName
	if sessionName == "" && len(sessions) == 1 {
		sessionName = sessions[0]
	}
	if sessionName == "" && len(sessions) > 1 {
		// Multiple sessions: ask user to pick via args or show list
		args := os.Args[3:] // ksw aws sso login [session]
		if len(args) > 0 {
			sessionName = args[0]
		} else {
			fmt.Printf("%s Multiple SSO sessions configured. Specify one:\n\n", logoStyle.Render("⎈ ksw aws sso login"))
			for _, s := range sessions {
				marker := ""
				if s == defaultName {
					marker = "  (default)"
				}
				fmt.Printf("  ksw aws sso login %s%s\n", s, marker)
			}
			os.Exit(0)
		}
	}
	if sessionName == "" {
		// Try from ~/.aws/config
		awsSessions := readAWSConfigSSOSessions()
		if len(awsSessions) == 1 {
			sessionName = awsSessions[0]
		} else if len(awsSessions) > 1 {
			fmt.Fprintf(os.Stderr, "%s No default SSO session. Run 'ksw aws sso config' to configure one.\n", warnStyle.Render("✗"))
			os.Exit(1)
		}
	}
	if sessionName == "" {
		fmt.Fprintf(os.Stderr, "%s No SSO sessions configured. Run 'ksw aws sso config' to create one.\n", warnStyle.Render("✗"))
		os.Exit(1)
	}

	fmt.Printf("\n  %s Running: %s\n\n",
		lipgloss.NewStyle().Foreground(lipgloss.Color("#bd93f9")).Bold(true).Render("⟳"),
		lipgloss.NewStyle().Foreground(lipgloss.Color("#f8f8f2")).Render("aws sso login --sso-session "+sessionName))

	cmd := exec.Command("aws", "sso", "login", "--sso-session", sessionName)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "\n%s aws sso login failed: %v\n",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555")).Render("✗"), err)
		os.Exit(1)
	}
	fmt.Printf("\n%s Logged in to '%s'\n",
		lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#50fa7b")).Render("✔"), sessionName)
}

func handleSSOConfig() {
	sessions, defaultName := listSSOSessions()
	activeSessions := detectActiveSSOSessions()
	awsConfigSessions := readAWSConfigSSOSessions()

	m := ssoConfigModel{
		step:              ssoStepMenu,
		sessions:          sessions,
		activeSessions:    activeSessions,
		awsConfigSessions: awsConfigSessions,
		defaultN:          defaultName,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
