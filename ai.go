package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ── AI Config ──────────────────────────────────────────

type aiConfig struct {
	Provider       string `json:"provider,omitempty"`        // openai | claude | gemini | bedrock
	APIKey         string `json:"api_key,omitempty"`         // for openai, claude, gemini
	Model          string `json:"model,omitempty"`
	AWSProfile     string `json:"aws_profile,omitempty"`     // for bedrock
	AWSRegion      string `json:"aws_region,omitempty"`      // for bedrock
	AWSAuthMethod  string `json:"aws_auth_method,omitempty"` // profile | keys | env
	AWSAccessKey   string `json:"aws_access_key,omitempty"`  // for bedrock keys auth
	AWSSecretKey   string `json:"aws_secret_key,omitempty"`  // for bedrock keys auth
}

// ── Conversational Memory ──────────────────────────────

type aiMemoryEntry struct {
	Query    string `json:"query"`
	Action   string `json:"action"`
	Result   string `json:"result"`
	Time     int64  `json:"time"`
}

const maxMemory = 10

// ── Response Cache ─────────────────────────────────────

type aiCache struct {
	Query    string `json:"query"`
	Response string `json:"response"`
	Time     int64  `json:"time"`
}

const cacheTTL = 30 // seconds

func cachePath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".ksw-cache.json")
}

func loadCache() *aiCache {
	data, err := os.ReadFile(cachePath())
	if err != nil {
		return nil
	}
	var c aiCache
	if err := json.Unmarshal(data, &c); err != nil {
		return nil
	}
	if time.Now().Unix()-c.Time > cacheTTL {
		return nil
	}
	return &c
}

func saveCache(query, response string) {
	c := aiCache{Query: query, Response: response, Time: time.Now().Unix()}
	data, _ := json.Marshal(c)
	_ = os.WriteFile(cachePath(), data, 0644)
}

// providerModels lists available models per provider (recommended first)
var providerModels = map[string][]string{
	"openai": {
		"gpt-4.1-mini",
		"gpt-4.1",
		"gpt-5",
	},
	"claude": {
		"claude-haiku-4-5-20251001",
		"claude-sonnet-4-5-20251001",
		"claude-opus-4-5-20251001",
	},
	"gemini": {
		"gemini-2.5-flash",
		"gemini-2.5-pro",
		"gemini-3-flash-preview",
	},
	"bedrock": {
		"us.anthropic.claude-sonnet-4-6",
		"us.anthropic.claude-opus-4-6-v1",
		"us.anthropic.claude-sonnet-4-5-20250929-v1:0",
		"us.anthropic.claude-opus-4-5-20251101-v1:0",
		"us.anthropic.claude-haiku-4-5-20251001-v1:0",
		"us.anthropic.claude-sonnet-4-20250514-v1:0",
		"us.amazon.nova-pro-v1:0",
		"us.amazon.nova-lite-v1:0",
		"us.amazon.nova-2-lite-v1:0",
		"us.amazon.nova-premier-v1:0",
	},
}

func defaultModel(provider string) string {
	if models, ok := providerModels[provider]; ok && len(models) > 0 {
		return models[0]
	}
	return ""
}

// ── Retry with backoff ─────────────────────────────────

const maxRetries = 3

// callWithRetry wraps an API call with retry logic for 429/5xx errors
func callWithRetry(fn func() (string, int, error)) (string, error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, statusCode, err := fn()
		if err == nil {
			return result, nil
		}
		// Retry on 429 (rate limit) or 5xx (server error)
		if statusCode == 429 || (statusCode >= 500 && statusCode < 600) {
			if attempt < maxRetries {
				wait := time.Duration(1<<uint(attempt)) * time.Second // 1s, 2s, 4s
				time.Sleep(wait)
				continue
			}
		}
		return "", err
	}
	return "", fmt.Errorf("max retries exceeded")
}

// ── handleAI ───────────────────────────────────────────

func handleAI(cfg config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: ksw ai \"<query>\"")
		fmt.Fprintln(os.Stderr, "       ksw ai config")
		os.Exit(1)
	}

	sub := os.Args[2]
	if sub == "config" {
		handleAIConfig(cfg)
		return
	}

	query := strings.Join(os.Args[2:], " ")

	if cfg.AI.Provider == "" {
		fmt.Fprintf(os.Stderr, "%s AI not configured. Run: ksw ai config\n", warnStyle.Render("✗"))
		os.Exit(1)
	}
	// Bedrock uses AWS creds, others need API key
	if cfg.AI.Provider != "bedrock" && cfg.AI.APIKey == "" {
		fmt.Fprintf(os.Stderr, "%s AI not configured. Run: ksw ai config\n", warnStyle.Render("✗"))
		os.Exit(1)
	}

	contexts, err := getContexts()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(contexts) == 0 {
		fmt.Fprintln(os.Stderr, "No contexts found in kubeconfig.")
		os.Exit(1)
	}

	// Check cache first
	if cached := loadCache(); cached != nil && strings.EqualFold(cached.Query, query) {
		executeRawResponse(cached.Response, contexts, &cfg)
		return
	}

	done := make(chan struct{})
	go showSpinner(done)

	candidates := preFilterContexts(query, contexts)
	if len(candidates) == 0 {
		candidates = contexts
	}

	chosen, raw, err := resolveContextWithAI(query, candidates, cfg)
	close(done)
	time.Sleep(90 * time.Millisecond)

	// Save cache
	if raw != "" {
		saveCache(query, raw)
	}

	if err != nil {
		if multiErr, ok := err.(*aiMultiError); ok {
			var results []string
			for _, act := range multiErr.actions {
				executeAction(act, contexts, &cfg)
				results = append(results, act.Action+":"+act.Command+act.Reply)
			}
			saveMemory(&cfg, query, "multi", strings.Join(results, " | "))
			return
		}
		if cmdErr, ok := err.(*aiCommandError); ok {
			saveMemory(&cfg, query, "command", cmdErr.command+" "+strings.Join(cmdErr.args, " "))
			runAICommand(cmdErr.command, cmdErr.args, cfg)
			return
		}
		if replyErr, ok := err.(*aiReplyError); ok {
			saveMemory(&cfg, query, "reply", replyErr.reply)
			fmt.Printf("🤖 %s\n", replyErr.reply)
			return
		}
		fmt.Fprintf(os.Stderr, "%s %v\n", warnStyle.Render("✗"), err)
		os.Exit(1)
	}

	current := getCurrentContext()
	if chosen == current {
		saveMemory(&cfg, query, "switch", "already on "+shortName(current))
		fmt.Printf("%s Already on %s\n", dimStyle.Render("·"), current)
		return
	}

	recordHistory(&cfg, current, chosen)
	if err := switchContext(chosen); err != nil {
		fmt.Fprintf(os.Stderr, "%s Failed to switch to '%s': %v\n", warnStyle.Render("✗"), chosen, err)
		os.Exit(1)
	}

	saveMemory(&cfg, query, "switch", shortName(chosen))
	_ = saveConfig(cfg)

	alias := ""
	for a, target := range cfg.Aliases {
		if target == chosen {
			alias = " " + aliasStyle.Render("@"+a)
			break
		}
	}
	fmt.Printf("%s Switched to %s%s\n", successStyle.Render("✔"), chosen, alias)
}

// executeAction runs a single AI action
func executeAction(act aiResponse, contexts []string, cfg *config) {
	switch act.Action {
	case "command":
		runAICommand(act.Command, act.Args, *cfg)
		// Reload config in case command modified it
		*cfg = loadConfig()
	case "switch":
		chosen, err := resolveExactOrFuzzy(act.Context, contexts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %v\n", warnStyle.Render("✗"), err)
			return
		}
		current := getCurrentContext()
		if chosen == current {
			fmt.Printf("%s Already on %s\n", dimStyle.Render("·"), current)
			return
		}
		recordHistory(cfg, current, chosen)
		if err := switchContext(chosen); err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to switch to '%s': %v\n", warnStyle.Render("✗"), chosen, err)
			return
		}
		_ = saveConfig(*cfg)
		fmt.Printf("%s Switched to %s\n", successStyle.Render("✔"), chosen)
	case "reply":
		fmt.Printf("🤖 %s\n", act.Reply)
	}
}

// executeRawResponse parses and executes a cached raw response
func executeRawResponse(raw string, contexts []string, cfg *config) {
	actions, err := parseAIResponse(raw)
	if err != nil {
		return
	}
	for _, act := range actions {
		executeAction(act, contexts, cfg)
	}
}

// saveMemory records an AI interaction in conversational memory
func saveMemory(cfg *config, query, action, result string) {
	entry := aiMemoryEntry{
		Query:  query,
		Action: action,
		Result: result,
		Time:   time.Now().Unix(),
	}
	cfg.AIMemory = append(cfg.AIMemory, entry)
	if len(cfg.AIMemory) > maxMemory {
		cfg.AIMemory = cfg.AIMemory[len(cfg.AIMemory)-maxMemory:]
	}
	_ = saveConfig(*cfg)
}

// ── handleAIConfig ─────────────────────────────────────

func handleAIConfig(cfg config) {
	providers := []string{"openai", "claude", "gemini", "bedrock"}

	fmt.Println(dimStyle.Render("  Configure AI provider for 'ksw ai'"))
	fmt.Println()

	fmt.Println("  Select provider:")
	for i, p := range providers {
		marker := "  "
		if p == cfg.AI.Provider {
			marker = "❯ "
		}
		fmt.Printf("  %s%d) %s\n", marker, i+1, p)
	}
	fmt.Printf("\n  Provider [1-%d]: ", len(providers))

	var choice string
	fmt.Scanln(&choice)
	idx := 0
	for _, c := range choice {
		if c >= '1' && c <= rune('0'+len(providers)) {
			idx = int(c-'0') - 1
		}
	}
	cfg.AI.Provider = providers[idx]

	if cfg.AI.Provider == "bedrock" {
		// Auth method selection
		fmt.Println("\n  Authentication method:")
		fmt.Println("  1) AWS Profile (aws cli / SSO)")
		fmt.Println("  2) Access Key + Secret Key")
		fmt.Println("  3) Environment variables (AWS_ACCESS_KEY_ID)")
		fmt.Printf("\n  Auth method [1-3]: ")
		var authChoice string
		fmt.Scanln(&authChoice)
		switch strings.TrimSpace(authChoice) {
		case "2":
			cfg.AI.AWSAuthMethod = "keys"
			fmt.Printf("  AWS Access Key ID: ")
			var ak string
			fmt.Scanln(&ak)
			ak = strings.TrimSpace(ak)
			if ak != "" {
				cfg.AI.AWSAccessKey = ak
			}
			fmt.Printf("  AWS Secret Access Key: ")
			var sk string
			fmt.Scanln(&sk)
			sk = strings.TrimSpace(sk)
			if sk != "" {
				cfg.AI.AWSSecretKey = sk
			}
		case "3":
			cfg.AI.AWSAuthMethod = "env"
			fmt.Println(dimStyle.Render("  Will use AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN from env"))
		default:
			cfg.AI.AWSAuthMethod = "profile"
			current := cfg.AI.AWSProfile
			if current == "" {
				current = "default"
			}
			fmt.Printf("  AWS Profile [%s]: ", current)
			var profile string
			fmt.Scanln(&profile)
			profile = strings.TrimSpace(profile)
			if profile != "" {
				cfg.AI.AWSProfile = profile
			} else if cfg.AI.AWSProfile == "" {
				cfg.AI.AWSProfile = "default"
			}
		}

		// AWS Region
		currentRegion := cfg.AI.AWSRegion
		if currentRegion == "" {
			currentRegion = "us-east-1"
		}
		fmt.Printf("  AWS Region [%s]: ", currentRegion)
		var region string
		fmt.Scanln(&region)
		region = strings.TrimSpace(region)
		if region != "" {
			cfg.AI.AWSRegion = region
		} else if cfg.AI.AWSRegion == "" {
			cfg.AI.AWSRegion = "us-east-1"
		}
	} else {
		// API Key
		fmt.Printf("  API Key for %s: ", cfg.AI.Provider)
		var apiKey string
		fmt.Scanln(&apiKey)
		apiKey = strings.TrimSpace(apiKey)
		if apiKey != "" {
			cfg.AI.APIKey = apiKey
		}
	}

	// Model selection
	models := providerModels[cfg.AI.Provider]
	fmt.Printf("\n  Select model for %s:\n", cfg.AI.Provider)
	currentModel := cfg.AI.Model
	if currentModel == "" {
		currentModel = defaultModel(cfg.AI.Provider)
	}
	for i, m := range models {
		marker := "  "
		if m == currentModel {
			marker = "❯ "
		}
		fmt.Printf("  %s%d) %s\n", marker, i+1, m)
	}
	fmt.Printf("\n  Model [1-%d]: ", len(models))

	var modelChoice string
	fmt.Scanln(&modelChoice)
	modelIdx := -1
	for _, c := range modelChoice {
		if c >= '1' && c <= '9' {
			n := int(c-'0') - 1
			if n < len(models) {
				modelIdx = n
			}
		}
	}
	if modelIdx >= 0 {
		cfg.AI.Model = models[modelIdx]
	} else {
		cfg.AI.Model = currentModel
	}

	if err := saveConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n%s AI configured: %s / %s\n", successStyle.Render("✔"),
		aliasStyle.Render(cfg.AI.Provider), dimStyle.Render(cfg.AI.Model))
}

// ── LLM resolution ─────────────────────────────────────

type aiResponse struct {
	Action  string   `json:"action"`
	Context string   `json:"context,omitempty"`
	Command string   `json:"command,omitempty"`
	Reply   string   `json:"reply,omitempty"`
	Args    []string `json:"args,omitempty"`
}

type aiCommandError struct {
	command string
	args    []string
}

func (e *aiCommandError) Error() string {
	return "command:" + e.command
}

type aiReplyError struct {
	reply string
}

func (e *aiReplyError) Error() string {
	return "reply:" + e.reply
}

// aiMultiError holds multiple actions to execute sequentially
type aiMultiError struct {
	actions []aiResponse
}

func (e *aiMultiError) Error() string {
	return fmt.Sprintf("multi:%d actions", len(e.actions))
}

// extractJSON finds the first valid JSON object or array in a string
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimSpace(s)

	// Find first '{' or '[' and match its closing pair
	startObj := strings.Index(s, "{")
	startArr := strings.Index(s, "[")

	start := startObj
	openChar := byte('{')
	closeChar := byte('}')
	if startArr >= 0 && (startObj < 0 || startArr < startObj) {
		start = startArr
		openChar = '['
		closeChar = ']'
	}
	if start < 0 {
		return s
	}

	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inString {
			escaped = true
			continue
		}
		if c == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if c == openChar {
			depth++
		} else if c == closeChar {
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return s[start:]
}

// parseAIResponse parses the LLM output into one or more aiResponse objects
func parseAIResponse(raw string) ([]aiResponse, error) {
	jsonStr := extractJSON(raw)

	// Try array first
	var multi []aiResponse
	if err := json.Unmarshal([]byte(jsonStr), &multi); err == nil && len(multi) > 0 {
		return multi, nil
	}

	// Try single object
	var single aiResponse
	if err := json.Unmarshal([]byte(jsonStr), &single); err == nil && single.Action != "" {
		return []aiResponse{single}, nil
	}

	return nil, fmt.Errorf("could not parse AI response: %s", truncate(raw, 200))
}

func resolveContextWithAI(query string, contexts []string, cfg config) (string, string, error) {
	ai := cfg.AI
	model := ai.Model
	if model == "" {
		model = defaultModel(ai.Provider)
	}

	prompt := buildPrompt(query, contexts, cfg)

	var raw string
	var err error

	switch ai.Provider {
	case "openai":
		raw, err = callWithRetry(func() (string, int, error) { return callOpenAI(prompt, model, ai.APIKey) })
	case "claude":
		raw, err = callWithRetry(func() (string, int, error) { return callClaude(prompt, model, ai.APIKey) })
	case "gemini":
		raw, err = callWithRetry(func() (string, int, error) { return callGemini(prompt, model, ai.APIKey) })
	case "bedrock":
		raw, err = callWithRetry(func() (string, int, error) { return callBedrock(prompt, model, ai) })
	default:
		return "", "", fmt.Errorf("unknown provider '%s'", ai.Provider)
	}
	if err != nil {
		return "", "", err
	}

	actions, err := parseAIResponse(raw)
	if err != nil {
		return "", raw, err
	}

	// Multiple actions → return multi error
	if len(actions) > 1 {
		return "", raw, &aiMultiError{actions: actions}
	}

	resp := actions[0]
	jsonStr, _ := json.Marshal(resp)

	switch resp.Action {
	case "command":
		return "", string(jsonStr), &aiCommandError{command: resp.Command, args: resp.Args}
	case "switch":
		result, err := resolveExactOrFuzzy(resp.Context, contexts)
		return result, string(jsonStr), err
	case "reply":
		return "", string(jsonStr), &aiReplyError{reply: resp.Reply}
	default:
		return "", string(jsonStr), fmt.Errorf("unexpected AI action: %s", resp.Action)
	}
}

func resolveExactOrFuzzy(result string, contexts []string) (string, error) {
	result = strings.TrimSpace(strings.Trim(result, `"'`))

	for _, ctx := range contexts {
		if ctx == result {
			return result, nil
		}
	}
	for _, ctx := range contexts {
		if shortName(ctx) == result {
			return ctx, nil
		}
	}

	var matches []string
	for _, ctx := range contexts {
		if strings.Contains(ctx, result) || strings.Contains(result, ctx) {
			matches = append(matches, ctx)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		fmt.Fprintf(os.Stderr, "%s Ambiguous result, did you mean one of these?\n", warnStyle.Render("?"))
		for i, m := range matches {
			fmt.Fprintf(os.Stderr, "  %d) %s\n", i+1, m)
		}
		fmt.Fprintf(os.Stderr, "\n  Select [1-%d]: ", len(matches))
		var pick string
		fmt.Scanln(&pick)
		for _, c := range pick {
			if c >= '1' && c <= '9' {
				n := int(c-'0') - 1
				if n < len(matches) {
					return matches[n], nil
				}
			}
		}
		return "", fmt.Errorf("invalid selection")
	}
	return "", fmt.Errorf("AI returned '%s' but no matching context found", result)
}

func buildPrompt(query string, contexts []string, cfg config) string {
	shorts := make([]string, len(contexts))
	for i, ctx := range contexts {
		shorts[i] = shortName(ctx)
	}
	list := strings.Join(shorts, "\n")

	// Build conversation history
	memoryBlock := ""
	if len(cfg.AIMemory) > 0 {
		var lines []string
		for _, m := range cfg.AIMemory {
			lines = append(lines, fmt.Sprintf("- User: \"%s\" → %s: %s", m.Query, m.Action, m.Result))
		}
		memoryBlock = fmt.Sprintf("\nRECENT CONVERSATION:\n%s\n", strings.Join(lines, "\n"))
	}

	// Build user state: groups, aliases, pins, history
	stateBlock := ""
	var stateParts []string

	// Groups
	if len(cfg.Groups) > 0 {
		var gLines []string
		for name, members := range cfg.Groups {
			shorts := make([]string, len(members))
			for i, m := range members {
				shorts[i] = shortName(m)
			}
			gLines = append(gLines, fmt.Sprintf("  %s: [%s]", name, strings.Join(shorts, ", ")))
		}
		stateParts = append(stateParts, "GROUPS:\n"+strings.Join(gLines, "\n"))
	} else {
		stateParts = append(stateParts, "GROUPS: none")
	}

	// Aliases
	if len(cfg.Aliases) > 0 {
		var aLines []string
		for alias, target := range cfg.Aliases {
			aLines = append(aLines, fmt.Sprintf("  @%s → %s", alias, shortName(target)))
		}
		stateParts = append(stateParts, "ALIASES:\n"+strings.Join(aLines, "\n"))
	} else {
		stateParts = append(stateParts, "ALIASES: none")
	}

	// Pins
	if len(cfg.Pins) > 0 {
		var pLines []string
		for _, p := range cfg.Pins {
			pLines = append(pLines, "  ★ "+shortName(p))
		}
		stateParts = append(stateParts, "PINNED:\n"+strings.Join(pLines, "\n"))
	} else {
		stateParts = append(stateParts, "PINNED: none")
	}

	// History
	if len(cfg.History) > 0 {
		var hLines []string
		for i, h := range cfg.History {
			hLines = append(hLines, fmt.Sprintf("  %d. %s", i+1, shortName(h)))
		}
		stateParts = append(stateParts, "HISTORY:\n"+strings.Join(hLines, "\n"))
	}

	stateBlock = "\nUSER STATE:\n" + strings.Join(stateParts, "\n") + "\n"

	currentCtx := getCurrentContext()
	currentShort := shortName(currentCtx)

	return fmt.Sprintf(`You are "ksw ai", an intelligent Kubernetes context switcher assistant created by Yonier Gomez.
You have full knowledge of the user's configuration and can manage everything.

CURRENT CONTEXT: %s
TOTAL CONTEXTS: %d
%s%s
RESPONSE FORMAT:
- Single action: return ONE JSON object
- Multiple actions: return a JSON ARRAY of objects
Examples:
  {"action":"command","command":"pin ls"}
  [{"action":"command","command":"pin ls"},{"action":"command","command":"group ls"}]

ACTIONS:
1. Switch context: {"action":"switch","context":"<exact short name from list>"}
2. Run command: {"action":"command","command":"<cmd>","args":["arg1","arg2",...]}
3. Free reply: {"action":"reply","reply":"<your answer in the user's language>"}

AVAILABLE COMMANDS (these execute real actions):
- "list" = list all contexts
- "group ls" = list groups
- "group add" args:["<name>","<pattern>"] = create group matching pattern
- "group rm" args:["<name>","<name2>",...] = remove one or more groups
- "group add-ctx" args:["<group>","<context short name>"] = add a context to an existing group (creates group if needed)
- "history" = show history
- "history N" = switch to history entry N (use command "history 3" not args)
- "alias add" args:["<alias>","<context short name>"] = create alias
- "alias rm" args:["<alias>"] = remove alias
- "alias ls" = list aliases
- "pin add" args:["<context short name>"] = pin a context
- "pin rm" args:["<context short name>"] = unpin
- "pin ls" = list pins
- "rename" args:["<old>","<new>"] = rename a context

RULES:
- Abbreviations: "ingti"="ingenieriati", "central"="integracioncentral", "canales"="canales-digitales"
- Environment suffixes: "dev"/"qa"/"pdn"/"prod" match cluster suffix
- When user asks MULTIPLE things, return a JSON ARRAY with all actions.
- When user asks to CREATE a group, DO IT with "command"+"group add". Don't just suggest.
- When user asks to ADD a context to a group, use "group add-ctx".
- When user asks to pin/alias/unpin/rename, DO IT. Don't just suggest.
- IMPORTANT: If user asks for a CUSTOM FORMAT (table, summary, resumen, tabla, comparar, etc.), use "reply" and build the answer yourself from USER STATE. Do NOT use "command" because commands have fixed output format.
- For questions/chat, use "reply" and answer naturally in the user's language. Use the USER STATE above to give accurate, specific answers.
- When user asks "who are you" or "what can you do", include specific details from their state (how many groups, pins, aliases they have).
- Pick the BEST single match for switch. Return short name EXACTLY as listed.
- Use conversation history to understand references like "the previous one", "same but dev", "go back".
- Return ONLY valid JSON. No text before or after.

Request: %s

Contexts:
%s

JSON:`, currentShort, len(contexts), stateBlock, memoryBlock, query, list)
}

func preFilterContexts(query string, contexts []string) []string {
	q := strings.ToLower(query)
	skip := map[string]bool{
		"ir": true, "a": true, "al": true, "el": true, "la": true, "de": true,
		"conectate": true, "conectar": true, "switch": true, "to": true, "go": true,
		"cambiar": true, "cambiate": true, "ve": true, "usa": true, "use": true,
		"lista": true, "listar": true, "show": true, "list": true, "mis": true, "my": true,
	}
	words := strings.Fields(q)
	var keywords []string
	for _, w := range words {
		if !skip[w] && len(w) > 1 {
			keywords = append(keywords, w)
		}
	}
	if len(keywords) == 0 {
		return contexts
	}
	var matches []string
	for _, ctx := range contexts {
		ctxLower := strings.ToLower(ctx)
		for _, kw := range keywords {
			if strings.Contains(ctxLower, kw) {
				matches = append(matches, ctx)
				break
			}
		}
	}
	return matches
}

func showSpinner(done <-chan struct{}) {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	dots := []string{"", ".", "..", "..."}
	i := 0
	for {
		select {
		case <-done:
			fmt.Printf("\r%-40s\r", "")
			return
		default:
			frame := dimStyle.Render(frames[i%len(frames)])
			d := dots[(i/3)%len(dots)]
			fmt.Printf("\r%s 🤖 Thinking%s   ", frame, d)
			i++
			time.Sleep(80 * time.Millisecond)
		}
	}
}

// ── OpenAI ─────────────────────────────────────────────

func callOpenAI(prompt, model, apiKey string) (string, int, error) {
	body := map[string]any{
		"model":       model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"max_tokens":  1000,
		"temperature": 0,
	}
	data, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient().Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("OpenAI request failed: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", resp.StatusCode, fmt.Errorf("OpenAI error %d: %s", resp.StatusCode, truncate(string(b), 200))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(b, &result); err != nil || len(result.Choices) == 0 {
		return "", 0, fmt.Errorf("unexpected OpenAI response")
	}
	return result.Choices[0].Message.Content, 200, nil
}

// ── Claude ─────────────────────────────────────────────

func callClaude(prompt, model, apiKey string) (string, int, error) {
	body := map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
		"max_tokens": 1000,
	}
	data, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(data))
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient().Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("Claude request failed: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", resp.StatusCode, fmt.Errorf("Claude error %d: %s", resp.StatusCode, truncate(string(b), 200))
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(b, &result); err != nil || len(result.Content) == 0 {
		return "", 0, fmt.Errorf("unexpected Claude response")
	}
	return result.Content[0].Text, 200, nil
}

// ── Gemini ─────────────────────────────────────────────

func callGemini(prompt, model, apiKey string) (string, int, error) {
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", model, apiKey)

	body := map[string]any{
		"contents": []map[string]any{
			{"parts": []map[string]string{{"text": prompt}}},
		},
		"generationConfig": map[string]any{
			"maxOutputTokens": 1000,
			"temperature":     0,
		},
	}
	data, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient().Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("Gemini request failed: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", resp.StatusCode, fmt.Errorf("Gemini error %d: %s", resp.StatusCode, truncate(string(b), 200))
	}

	var result struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return "", 0, fmt.Errorf("unexpected Gemini response: %w", err)
	}
	if result.PromptFeedback.BlockReason != "" {
		return "", 0, fmt.Errorf("Gemini blocked: %s", result.PromptFeedback.BlockReason)
	}
	if len(result.Candidates) == 0 {
		return "", 0, fmt.Errorf("empty Gemini response: %s", truncate(string(b), 300))
	}
	parts := result.Candidates[0].Content.Parts
	if len(parts) == 0 {
		return "", 0, fmt.Errorf("empty Gemini response (finishReason: %s)", result.Candidates[0].FinishReason)
	}
	return parts[0].Text, 200, nil
}

// ── Bedrock (AWS SigV4) ────────────────────────────────

func callBedrock(prompt, modelID string, ai aiConfig) (string, int, error) {
	region := ai.AWSRegion
	if region == "" {
		region = "us-east-1"
	}

	// Build messages JSON for --messages parameter
	messages, _ := json.Marshal([]map[string]any{
		{
			"role": "user",
			"content": []map[string]any{
				{"text": prompt},
			},
		},
	})

	inferenceConfig, _ := json.Marshal(map[string]any{
		"maxTokens":   1000,
		"temperature": 0.0,
	})

	// Use aws cli to call bedrock — handles SigV4, SSO, profiles correctly
	args := []string{
		"bedrock-runtime", "converse",
		"--model-id", modelID,
		"--region", region,
		"--messages", string(messages),
		"--inference-config", string(inferenceConfig),
		"--output", "json",
	}

	// Set profile/credentials based on auth method
	env := os.Environ()
	switch ai.AWSAuthMethod {
	case "keys":
		env = append(env,
			"AWS_ACCESS_KEY_ID="+ai.AWSAccessKey,
			"AWS_SECRET_ACCESS_KEY="+ai.AWSSecretKey,
		)
	case "env":
		// env vars already in os.Environ()
	default:
		// profile
		if ai.AWSProfile != "" && ai.AWSProfile != "default" {
			args = append(args, "--profile", ai.AWSProfile)
		}
	}

	cmd := exec.Command("aws", args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if strings.Contains(msg, "ThrottlingException") || strings.Contains(msg, "Too Many Requests") {
			return "", 429, fmt.Errorf("Bedrock throttled: %s", truncate(msg, 200))
		}
		if strings.Contains(msg, "InternalServerException") || strings.Contains(msg, "ServiceUnavailable") {
			return "", 500, fmt.Errorf("Bedrock server error: %s", truncate(msg, 200))
		}
		return "", 0, fmt.Errorf("Bedrock error: %s", truncate(msg, 300))
	}

	// Parse aws cli JSON output
	var result struct {
		Output struct {
			Message struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		} `json:"output"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return "", 0, fmt.Errorf("unexpected Bedrock response: %w", err)
	}
	if len(result.Output.Message.Content) == 0 {
		return "", 0, fmt.Errorf("empty Bedrock response")
	}
	return result.Output.Message.Content[0].Text, 200, nil
}

// ── Helpers ────────────────────────────────────────────

func httpClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// runAICommand executes a ksw command suggested by the AI
func runAICommand(command string, args []string, cfg config) {
	// Handle "history N" — switch to history entry
	if strings.HasPrefix(command, "history ") {
		parts := strings.Fields(command)
		if len(parts) == 2 {
			n := 0
			for _, c := range parts[1] {
				if c >= '0' && c <= '9' {
					n = n*10 + int(c-'0')
				}
			}
			if n >= 1 && n <= len(cfg.History) {
				target := cfg.History[n-1]
				current := getCurrentContext()
				recordHistory(&cfg, current, target)
				if err := switchContext(target); err != nil {
					fmt.Fprintf(os.Stderr, "%s Context '%s' not found.\n", warnStyle.Render("✗"), target)
					os.Exit(1)
				}
				_ = saveConfig(cfg)
				fmt.Printf("%s Switched to %s\n", successStyle.Render("✔"), target)
				return
			}
		}
	}

	switch command {
	case "list":
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
		fmt.Printf(dimStyle.Render("  %d contexts:")+"\n", len(contexts))
		for _, ctx := range contexts {
			alias := ""
			if a, ok := reverseAlias[ctx]; ok {
				alias = " " + aliasStyle.Render("@"+a)
			}
			if ctx == current {
				fmt.Printf("  %s%s %s\n", currentValueStyle.Render("▸ "+ctx), alias, activeTag)
			} else {
				fmt.Printf("    %s%s\n", ctx, alias)
			}
		}

	case "group ls":
		os.Args = []string{"ksw", "group", "ls"}
		handleGroup(cfg)

	case "group add":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%s group add needs name and pattern\n", warnStyle.Render("✗"))
			return
		}
		groupName := args[0]
		pattern := strings.ToLower(args[1])
		// Strip glob chars for substring matching — AI sometimes sends "eks-sufi-*"
		cleanPattern := strings.TrimRight(strings.TrimLeft(pattern, "*"), "*")
		// Find all contexts matching the pattern (substring or glob)
		contexts, err := getContexts()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		var members []string
		for _, ctx := range contexts {
			ctxLower := strings.ToLower(ctx)
			if strings.Contains(ctxLower, cleanPattern) {
				members = append(members, ctx)
			}
		}
		if len(members) == 0 {
			fmt.Fprintf(os.Stderr, "%s No contexts match '%s'\n", warnStyle.Render("✗"), pattern)
			return
		}
		cfg.Groups[groupName] = members
		_ = saveConfig(cfg)
		fmt.Printf("%s Group '%s' created (%d contexts)\n", successStyle.Render("✔"), groupName, len(members))
		for _, m := range members {
			fmt.Printf("    %s %s\n", dimStyle.Render("·"), m)
		}

	case "group rm":
		if len(args) < 1 {
			fmt.Fprintf(os.Stderr, "%s group rm needs a name\n", warnStyle.Render("✗"))
			return
		}
		for _, name := range args {
			if _, ok := cfg.Groups[name]; !ok {
				fmt.Fprintf(os.Stderr, "%s Group '%s' not found\n", warnStyle.Render("✗"), name)
				continue
			}
			delete(cfg.Groups, name)
			fmt.Printf("%s Group '%s' removed\n", successStyle.Render("✔"), name)
		}
		_ = saveConfig(cfg)

	case "group add-ctx":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%s group add-ctx needs group name and context\n", warnStyle.Render("✗"))
			return
		}
		groupName := args[0]
		target := args[1]
		contexts, _ := getContexts()
		resolved := ""
		for _, ctx := range contexts {
			if shortName(ctx) == target || ctx == target || strings.Contains(strings.ToLower(ctx), strings.ToLower(target)) {
				resolved = ctx
				break
			}
		}
		if resolved == "" {
			fmt.Fprintf(os.Stderr, "%s Context '%s' not found\n", warnStyle.Render("✗"), target)
			return
		}
		// Create group if it doesn't exist
		if cfg.Groups[groupName] == nil {
			cfg.Groups[groupName] = []string{}
		}
		// Check duplicate
		for _, c := range cfg.Groups[groupName] {
			if c == resolved {
				fmt.Printf("%s Already in group '%s': %s\n", dimStyle.Render("·"), groupName, resolved)
				return
			}
		}
		cfg.Groups[groupName] = append(cfg.Groups[groupName], resolved)
		_ = saveConfig(cfg)
		fmt.Printf("%s Added %s to group '%s'\n", successStyle.Render("✔"), shortName(resolved), groupName)

	case "rename":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%s rename needs old and new name\n", warnStyle.Render("✗"))
			return
		}
		oldName := args[0]
		newName := args[1]
		// Resolve old name
		contexts, _ := getContexts()
		resolved := ""
		for _, ctx := range contexts {
			if shortName(ctx) == oldName || ctx == oldName || strings.Contains(ctx, oldName) {
				resolved = ctx
				break
			}
		}
		if resolved == "" {
			fmt.Fprintf(os.Stderr, "%s Context '%s' not found\n", warnStyle.Render("✗"), oldName)
			return
		}
		cmd := exec.Command("kubectl", "config", "rename-context", resolved, newName)
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "%s Failed to rename: %s\n", warnStyle.Render("✗"), strings.TrimSpace(string(out)))
			return
		}
		// Update aliases/history
		for alias, target := range cfg.Aliases {
			if target == resolved {
				cfg.Aliases[alias] = newName
			}
		}
		for i, h := range cfg.History {
			if h == resolved {
				cfg.History[i] = newName
			}
		}
		_ = saveConfig(cfg)
		fmt.Printf("%s Renamed %s → %s\n", successStyle.Render("✔"), dimStyle.Render(resolved), currentValueStyle.Render(newName))

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

	case "alias add":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%s alias add needs name and context\n", warnStyle.Render("✗"))
			return
		}
		aliasName := strings.TrimLeft(args[0], "@")
		target := args[1]
		// Resolve short name to full context
		contexts, _ := getContexts()
		resolved := ""
		for _, ctx := range contexts {
			if shortName(ctx) == target || ctx == target || strings.Contains(ctx, target) {
				resolved = ctx
				break
			}
		}
		if resolved == "" {
			fmt.Fprintf(os.Stderr, "%s Context '%s' not found\n", warnStyle.Render("✗"), target)
			return
		}
		cfg.Aliases[aliasName] = resolved
		_ = saveConfig(cfg)
		fmt.Printf("%s Alias @%s → %s\n", successStyle.Render("✔"), aliasName, resolved)

	case "alias rm":
		if len(args) < 1 {
			return
		}
		name := strings.TrimLeft(args[0], "@")
		if _, ok := cfg.Aliases[name]; !ok {
			fmt.Fprintf(os.Stderr, "%s Alias '%s' not found\n", warnStyle.Render("✗"), name)
			return
		}
		delete(cfg.Aliases, name)
		_ = saveConfig(cfg)
		fmt.Printf("%s Alias @%s removed\n", successStyle.Render("✔"), name)

	case "alias ls":
		os.Args = []string{"ksw", "alias", "ls"}
		handleAlias(cfg)

	case "pin add":
		if len(args) < 1 {
			return
		}
		target := args[0]
		contexts, _ := getContexts()
		resolved := ""
		for _, ctx := range contexts {
			if shortName(ctx) == target || ctx == target || strings.Contains(ctx, target) {
				resolved = ctx
				break
			}
		}
		if resolved == "" {
			fmt.Fprintf(os.Stderr, "%s Context '%s' not found\n", warnStyle.Render("✗"), target)
			return
		}
		cfg.Pins = append(cfg.Pins, resolved)
		_ = saveConfig(cfg)
		fmt.Printf("%s Pinned %s\n", successStyle.Render("✔"), resolved)

	case "pin rm":
		if len(args) < 1 {
			return
		}
		target := args[0]
		newPins := make([]string, 0, len(cfg.Pins))
		found := false
		for _, p := range cfg.Pins {
			if strings.Contains(p, target) || shortName(p) == target {
				found = true
				continue
			}
			newPins = append(newPins, p)
		}
		if !found {
			fmt.Fprintf(os.Stderr, "%s '%s' not pinned\n", warnStyle.Render("✗"), target)
			return
		}
		cfg.Pins = newPins
		_ = saveConfig(cfg)
		fmt.Printf("%s Unpinned %s\n", successStyle.Render("✔"), target)

	case "pin ls":
		os.Args = []string{"ksw", "pin", "ls"}
		handlePin(cfg)

	default:
		fmt.Fprintf(os.Stderr, "%s Command '%s' not supported via AI yet.\n", warnStyle.Render("?"), command)
	}
}
