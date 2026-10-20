package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ── Detalle de profiles ────────────────────────────────

// profileDetail es la información SSO de un [profile X] en ~/.aws/config.
type profileDetail struct {
	Name      string
	AccountID string
	Session   string // sso_session
	Valid     bool   // true si AccountID tiene 12 dígitos
}

var profileHeaderRe = regexp.MustCompile(`^\[profile\s+([^\]]+)\]$`)

// parseProfileDetails parsea (sin I/O) los profiles SSO de un ~/.aws/config.
// Solo incluye profiles que declaran sso_account_id.
func parseProfileDetails(content string) map[string]profileDetail {
	out := make(map[string]profileDetail)
	var cur string
	var d profileDetail
	flush := func() {
		if cur != "" && d.AccountID != "" {
			d.Name = cur
			out[cur] = d
		}
	}
	for _, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "[") {
			flush()
			cur, d = "", profileDetail{}
			if m := profileHeaderRe.FindStringSubmatch(line); m != nil {
				cur = strings.TrimSpace(m[1])
			}
			continue
		}
		if cur == "" || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		key, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		switch key {
		case "sso_account_id":
			d.AccountID = val
			d.Valid = isValidAccountID(val)
		case "sso_session":
			d.Session = val
		}
	}
	flush()
	return out
}

// readProfileDetails lee ~/.aws/config y retorna el detalle de sus profiles SSO.
func readProfileDetails() map[string]profileDetail {
	data, err := os.ReadFile(awsConfigPath())
	if err != nil {
		return map[string]profileDetail{}
	}
	return parseProfileDetails(string(data))
}

// ── Cuentas que ya no existen en SSO ───────────────────

// staleAccount es una cuenta AWS que tiene profile(s) en ~/.aws/config pero
// que ya no aparece en la lista de cuentas de AWS SSO.
type staleAccount struct {
	AccountID string
	Profiles  []string // profiles de ~/.aws/config que apuntan a la cuenta
	Session   string
	Contexts  []string // contextos EKS (ARN) de la cuenta presentes en el kubeconfig
}

// accountIDFromContext extrae el account ID de un contexto EKS con formato ARN
// (arn:aws:eks:<region>:<account>:cluster/<name>). Retorna "" si no aplica.
func accountIDFromContext(ctx string) string {
	parts := strings.Split(ctx, ":")
	if len(parts) < 6 || parts[0] != "arn" {
		return ""
	}
	return parts[4]
}

// clusterNameFromContext retorna el nombre del cluster de un contexto ARN.
func clusterNameFromContext(ctx string) string {
	if idx := strings.LastIndex(ctx, "/"); idx >= 0 {
		return ctx[idx+1:]
	}
	return ctx
}

// regionFromContext retorna la región de un contexto ARN.
func regionFromContext(ctx string) string {
	parts := strings.Split(ctx, ":")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

// findStaleAccounts compara los profiles de una sesión SSO con la lista de cuentas
// que AWS SSO devuelve hoy. Función pura (sin I/O).
//
// Reglas de seguridad:
//   - Si SSO devolvió 0 cuentas no se marca nada (casi seguro es un error, no una baja masiva).
//   - Solo se evalúan profiles con sso_session == session; los de otras sesiones no se tocan.
//   - Los clusters (Contexts) se ofrecen únicamente si ningún otro profile que se conserva
//     sigue apuntando a esa misma cuenta.
func findStaleAccounts(details map[string]profileDetail, session string, accounts []ssoAccount, contexts map[string]bool) []staleAccount {
	if len(accounts) == 0 || session == "" {
		return nil
	}
	inSSO := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		inSSO[a.AccountID] = true
	}

	names := make([]string, 0, len(details))
	for n := range details {
		names = append(names, n)
	}
	sort.Strings(names)

	byAccount := make(map[string]*staleAccount)
	var order []string
	staleNames := make(map[string]bool)
	for _, n := range names {
		d := details[n]
		if d.Session != session || !d.Valid || inSSO[d.AccountID] {
			continue
		}
		staleNames[n] = true
		sa, ok := byAccount[d.AccountID]
		if !ok {
			sa = &staleAccount{AccountID: d.AccountID, Session: session}
			byAccount[d.AccountID] = sa
			order = append(order, d.AccountID)
		}
		sa.Profiles = append(sa.Profiles, n)
	}

	// Cuentas que siguen referenciadas por profiles que NO se van a eliminar
	kept := make(map[string]bool)
	for n, d := range details {
		if !staleNames[n] && d.Valid {
			kept[d.AccountID] = true
		}
	}

	var out []staleAccount
	for _, id := range order {
		sa := byAccount[id]
		if !kept[id] {
			for ctx := range contexts {
				if accountIDFromContext(ctx) == id {
					sa.Contexts = append(sa.Contexts, ctx)
				}
			}
			sort.Strings(sa.Contexts)
		}
		out = append(out, *sa)
	}
	return out
}

// detectStaleAccounts consulta AWS SSO por cada sesión usada por los profiles y
// retorna las cuentas que ya no existen. Si una sesión no tiene token válido se
// omite y se explica en note (no es un error: el usuario simplemente no hizo login).
func detectStaleAccounts() (stale []staleAccount, note string) {
	details := readProfileDetails()
	if len(details) == 0 {
		return nil, ""
	}
	sessionSet := make(map[string]bool)
	for _, d := range details {
		if d.Session != "" {
			sessionSet[d.Session] = true
		}
	}
	sessions := make([]string, 0, len(sessionSet))
	for s := range sessionSet {
		sessions = append(sessions, s)
	}
	sort.Strings(sessions)

	contexts, err := getExistingEKSContexts()
	if err != nil {
		contexts = map[string]bool{}
	}

	var notes []string
	for _, s := range sessions {
		accounts, matched, err := listSSOAccountsForSession(s)
		if err != nil || !matched {
			notes = append(notes, fmt.Sprintf("session '%s' skipped (not logged in)", s))
			continue
		}
		stale = append(stale, findStaleAccounts(details, s, accounts, contexts)...)
	}
	if len(notes) > 0 {
		note = "Account check " + strings.Join(notes, ", ")
	}
	return stale, note
}

// ── Cuentas SSO por sesión ─────────────────────────────

// resolveSSOSession retorna start URL y región SSO de una sesión, leyendo
// primero ~/.aws/config y luego ~/.ksw.json.
func resolveSSOSession(sessionName string) (name, startURL, region string) {
	if sessionName == "" {
		sessionName = loadSSOSettings().SessionName
	}
	if s, ok := readAWSConfigSSOSessionDetails()[sessionName]; ok && s.startURL != "" {
		region = s.region
		startURL = s.startURL
	}
	cfg := loadSSOSettingsFor(sessionName)
	if startURL == "" {
		startURL = cfg.StartURL
	}
	if region == "" {
		region = cfg.Region
	}
	return sessionName, startURL, region
}

func normalizeStartURL(u string) string {
	return strings.TrimRight(strings.TrimSpace(u), "/#")
}

func parseTokenExpiry(s string) time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05UTC", "2006-01-02T15:04:05Z", "2006-01-02T15:04:05.000Z"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// findSSOAccessToken busca en ~/.aws/sso/cache el accessToken de la sesión con ese start URL.
// Si hay varios candidatos elige el de mayor expiración (antes se tomaba el primer archivo en
// orden alfabético, que depende del hash del nombre y por eso fallaba solo en algunos equipos).
// matched=false indica que ningún token coincide con el start URL y se devolvió otro (legacy).
func findSSOAccessToken(startURL string) (token string, expires time.Time, matched bool) {
	home, _ := os.UserHomeDir()
	cacheDir := filepath.Join(home, ".aws", "sso", "cache")
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return "", time.Time{}, false
	}
	want := normalizeStartURL(startURL)
	var bestMatch, anyToken string
	var bestExp, anyExp time.Time
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(cacheDir, e.Name()))
		if err != nil {
			continue
		}
		var cache map[string]interface{}
		if json.Unmarshal(data, &cache) != nil {
			continue
		}
		t, _ := cache["accessToken"].(string)
		if t == "" {
			continue
		}
		exp := time.Time{}
		if s, ok := cache["expiresAt"].(string); ok {
			exp = parseTokenExpiry(s)
		}
		if anyToken == "" || exp.After(anyExp) {
			anyToken, anyExp = t, exp
		}
		u, _ := cache["startUrl"].(string)
		if u == "" || normalizeStartURL(u) != want {
			continue
		}
		if bestMatch == "" || exp.After(bestExp) {
			bestMatch, bestExp = t, exp
		}
	}
	if bestMatch != "" {
		return bestMatch, bestExp, true
	}
	return anyToken, anyExp, false
}

// firstProfileOfSession retorna un profile que usa la sesión SSO indicada ("" si no hay).
func firstProfileOfSession(session string) string {
	details := readProfileDetails()
	var names []string
	for n, d := range details {
		if d.Session == session {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// firstLine retorna la primera línea no vacía de s, recortada a max caracteres.
func firstLine(s string, max int) string {
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l != "" {
			if len(l) > max {
				l = l[:max] + "…"
			}
			return l
		}
	}
	return ""
}

// listSSOAccountsForSession lista las cuentas de AWS SSO usando el token de la sesión indicada
// (o la default si sessionName está vacío). matched indica si el token corresponde exactamente
// a esa sesión; los reportes de cuentas eliminadas solo deben confiar en resultados con matched=true.
//
// Si el access token en caché está vencido (o por vencer) intenta renovarlo con el refresh token
// ejecutando una llamada AWS con un profile de la sesión, antes de fallar pidiendo un nuevo login.
func listSSOAccountsForSession(sessionName string) ([]ssoAccount, bool, error) {
	name, startURL, region := resolveSSOSession(sessionName)
	token, exp, matched := findSSOAccessToken(startURL)
	if token == "" || (!exp.IsZero() && time.Until(exp) < time.Minute) {
		if p := firstProfileOfSession(name); p != "" {
			refreshSSOToken(p)
			token, exp, matched = findSSOAccessToken(startURL)
		}
	}
	if token == "" {
		return nil, false, fmt.Errorf("no valid SSO token. Run: aws sso login --sso-session %s", name)
	}

	var lastErr error
	for attempt := 0; attempt < awsMaxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 700 * time.Millisecond)
		}
		cmd := exec.Command("aws", "sso", "list-accounts", "--access-token", token, "--region", region, "--output", "json")
		cmd.Env = append(os.Environ(), "AWS_PAGER=")
		out, err := cmd.Output()
		if err != nil {
			detail := ""
			if ee, ok := err.(*exec.ExitError); ok {
				detail = firstLine(string(ee.Stderr), 140)
			}
			msg := fmt.Sprintf("failed to list SSO accounts (region %s). Run: aws sso login --sso-session %s", region, name)
			if detail != "" {
				msg += " — " + detail
			}
			lastErr = fmt.Errorf("%s", msg)
			if isTransientAWSError(detail) {
				continue
			}
			return nil, matched, lastErr
		}
		var resp ssoListResponse
		if err := json.Unmarshal(out, &resp); err != nil {
			return nil, matched, fmt.Errorf("failed to parse SSO response: %w", err)
		}
		return resp.AccountList, matched, nil
	}
	return nil, matched, lastErr
}

// ── Eliminación ────────────────────────────────────────

// removeProfileSections elimina de ~/.aws/config los bloques [profile X] indicados.
// No toca ningún otro bloque ni comentario fuera de esas secciones.
func removeProfileSections(names []string) error {
	if len(names) == 0 {
		return nil
	}
	drop := make(map[string]bool, len(names))
	for _, n := range names {
		drop[n] = true
	}
	configFile := awsConfigPath()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	var out []string
	skip := false
	for _, l := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "[") {
			skip = false
			if m := profileHeaderRe.FindStringSubmatch(trimmed); m != nil && drop[strings.TrimSpace(m[1])] {
				skip = true
			}
		}
		if !skip {
			out = append(out, l)
		}
	}
	return os.WriteFile(configFile, []byte(strings.Join(out, "\n")), 0644)
}

// mainKubeconfigPath retorna el kubeconfig principal ($KUBECONFIG o ~/.kube/config).
func mainKubeconfigPath() string {
	if env := os.Getenv("KUBECONFIG"); env != "" {
		first := strings.Split(env, string(os.PathListSeparator))[0]
		if first != "" {
			return first
		}
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".kube", "config")
}

// staleRemoval resume el resultado de eliminar cuentas obsoletas.
type staleRemoval struct {
	Accounts int
	Profiles int
	Clusters int
	Backup   string
	Err      error
}

// removeStaleAccounts elimina los profiles de las cuentas indicadas de ~/.aws/config
// y, si withClusters es true, también sus contextos EKS del kubeconfig.
// Crea un backup de ~/.aws/config antes de modificarlo.
func removeStaleAccounts(accounts []staleAccount, withClusters bool) staleRemoval {
	res := staleRemoval{Accounts: len(accounts)}
	var profiles, contexts []string
	seenCtx := make(map[string]bool)
	for _, a := range accounts {
		profiles = append(profiles, a.Profiles...)
		for _, c := range a.Contexts {
			if !seenCtx[c] {
				seenCtx[c] = true
				contexts = append(contexts, c)
			}
		}
	}

	backup, err := createAWSConfigBackup()
	if err != nil {
		res.Err = fmt.Errorf("backup failed: %w", err)
		return res
	}
	res.Backup = backup

	if err := removeProfileSections(profiles); err != nil {
		res.Err = fmt.Errorf("cannot update %s: %w", awsConfigPath(), err)
		return res
	}
	res.Profiles = len(profiles)

	if withClusters && len(contexts) > 0 {
		current := ""
		if out, err := exec.Command("kubectl", "config", "current-context").Output(); err == nil {
			current = strings.TrimSpace(string(out))
		}
		removed, _ := removeStaleClusters(contexts)
		res.Clusters = removed
		if current != "" && seenCtx[current] {
			// Evitar current-context apuntando a un contexto que ya no existe
			_ = exec.Command("kubectl", "config", "unset", "current-context").Run()
		}
	}
	return res
}

// countStaleClusters suma los contextos EKS asociados a las cuentas indicadas.
func countStaleClusters(accounts []staleAccount) int {
	seen := make(map[string]bool)
	for _, a := range accounts {
		for _, c := range a.Contexts {
			seen[c] = true
		}
	}
	return len(seen)
}
