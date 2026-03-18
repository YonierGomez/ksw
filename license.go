package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	lsVerifyURL   = "https://api.lemonsqueezy.com/v1/licenses/validate"
	lsActivateURL = "https://api.lemonsqueezy.com/v1/licenses/activate"
	lsDeactivateURL = "https://api.lemonsqueezy.com/v1/licenses/deactivate"
	licCacheTTL   = 24 * time.Hour
)

// licenseData se guarda en ~/.ksw.json — el key va cifrado con AES
type licenseData struct {
	KeyEnc      string `json:"key_enc"`            // key cifrado en hex (AES-GCM)
	Email       string `json:"email,omitempty"`
	InstanceID  string `json:"instance_id,omitempty"`
	ActivatedAt string `json:"activated_at,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	CacheValid  bool   `json:"cache_valid,omitempty"`
	CachedAt    string `json:"cached_at,omitempty"`
}

type lsValidateResponse struct {
	Valid bool   `json:"valid"`
	Error string `json:"error,omitempty"`
	LicenseKey struct {
		Key    string `json:"key"`
		Status string `json:"status"`
	} `json:"license_key"`
	Instance struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"instance"`
	Meta struct {
		CustomerEmail string `json:"customer_email"`
		ProductName   string `json:"product_name"`
	} `json:"meta"`
}

type lsActivateResponse struct {
	Activated bool   `json:"activated"`
	Error     string `json:"error,omitempty"`
	Instance  struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"instance"`
	LicenseKey struct {
		Key    string `json:"key"`
		Status string `json:"status"`
	} `json:"license_key"`
	Meta struct {
		CustomerEmail string `json:"customer_email"`
		ProductName   string `json:"product_name"`
	} `json:"meta"`
}

// readMasked lee input del usuario mostrando * por cada carácter, soporta backspace
func readMasked() string {
	// Guardar estado actual del terminal
	getState := exec.Command("stty", "-g")
	getState.Stdin = os.Stdin
	oldState, err := getState.Output()

	if err == nil {
		// Poner en modo raw sin echo
		raw := exec.Command("stty", "raw", "-echo")
		raw.Stdin = os.Stdin
		_ = raw.Run()
		defer func() {
			restore := exec.Command("stty", strings.TrimSpace(string(oldState)))
			restore.Stdin = os.Stdin
			_ = restore.Run()
		}()
	}

	var chars []rune
	buf := make([]byte, 1)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil || n == 0 {
			break
		}
		b := buf[0]
		if b == '\r' || b == '\n' {
			break
		}
		if b == 3 { // Ctrl+C
			fmt.Println()
			os.Exit(0)
		}
		if b == 127 || b == 8 { // Backspace
			if len(chars) > 0 {
				chars = chars[:len(chars)-1]
				fmt.Print("\b \b")
			}
			continue
		}
		if b < 32 {
			continue
		}
		chars = append(chars, rune(b))
		fmt.Print("*")
	}
	return strings.TrimSpace(string(chars))
}


func machineFingerprint() string {
	hostname, _ := os.Hostname()
	home, _ := os.UserHomeDir()
	raw := hostname + "|" + home
	sum := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", sum[:8])
}

// aesKey deriva una clave AES-256 del fingerprint
func aesKey() []byte {
	sum := sha256.Sum256([]byte("ksw-license|" + machineFingerprint()))
	return sum[:]
}

// encryptKey cifra el license key con AES-GCM usando el fingerprint como clave
func encryptKey(plainKey string) (string, error) {
	block, err := aes.NewCipher(aesKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nonce, nonce, []byte(plainKey), nil)
	return hex.EncodeToString(ciphertext), nil
}

// decryptKey descifra el license key
func decryptKey(encHex string) (string, error) {
	data, err := hex.DecodeString(encHex)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(aesKey())
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(data) < gcm.NonceSize() {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := data[:gcm.NonceSize()], data[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// isLicenseValid verifica la licencia — usa caché local de 24h para evitar HTTP en cada comando
func isLicenseValid() bool {
	cfg := loadConfig()
	lic := cfg.License
	if lic.KeyEnc == "" || lic.InstanceID == "" {
		return false
	}
	// Verificar que el archivo pertenece a esta máquina
	if lic.Fingerprint != "" && lic.Fingerprint != machineFingerprint() {
		return false
	}
	// Usar caché si está vigente (< 24h)
	if lic.CachedAt != "" {
		if t, err := time.Parse(time.RFC3339, lic.CachedAt); err == nil {
			if time.Since(t) < licCacheTTL {
				return lic.CacheValid
			}
		}
	}
	// Descifrar key para validar
	key, err := decryptKey(lic.KeyEnc)
	if err != nil {
		return false
	}
	valid := validateWithLS(key, lic.InstanceID)
	// Guardar resultado en caché
	cfg2 := loadConfig()
	cfg2.License.CacheValid = valid
	cfg2.License.CachedAt = time.Now().Format(time.RFC3339)
	_ = saveConfig(cfg2)
	return valid
}

// validateWithLS llama a la API de Lemon Squeezy para verificar la licencia
func validateWithLS(key, instanceID string) bool {
	data := url.Values{}
	data.Set("license_key", key)
	data.Set("instance_id", instanceID)

	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.PostForm(lsVerifyURL, data)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result lsValidateResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return false
	}
	return result.Valid
}

// activateLicense activa la key en LS y la guarda cifrada localmente.
// Si ya hay una instancia activa en LS para este fingerprint, la desactiva primero.
func activateLicense(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("license key cannot be empty")
	}

	finger := machineFingerprint()
	instanceName := "ksw-" + finger

	client := &http.Client{Timeout: 10 * time.Second}

	// Intentar activar
	result, err := doActivate(client, key, instanceName)
	if err != nil {
		return err
	}

	// Si ya estaba activada (limite alcanzado), intentar desactivar instancia anterior y reintentar
	if !result.Activated {
		errMsg := strings.ToLower(result.Error)
		if strings.Contains(errMsg, "limit") || strings.Contains(errMsg, "already") || strings.Contains(errMsg, "exceeded") {
			// Buscar instance_id guardado previamente para desactivar
			cfg := loadConfig()
			oldInstanceID := cfg.License.InstanceID
			if oldInstanceID != "" {
				oldKey, _ := decryptKey(cfg.License.KeyEnc)
				if oldKey == "" {
					oldKey = key
				}
				_ = doDeactivate(client, oldKey, oldInstanceID)
			}
			// Reintentar activación
			result, err = doActivate(client, key, instanceName)
			if err != nil {
				return err
			}
		}
		if !result.Activated {
			msg := result.Error
			if msg == "" {
				msg = "activation failed"
			}
			return fmt.Errorf("%s", msg)
		}
	}

	// Cifrar el key antes de guardar
	encKey, err := encryptKey(key)
	if err != nil {
		return fmt.Errorf("could not secure license: %w", err)
	}

	cfg := loadConfig()
	cfg.License = licenseData{
		KeyEnc:      encKey,
		Email:       result.Meta.CustomerEmail,
		InstanceID:  result.Instance.ID,
		ActivatedAt: time.Now().Format(time.RFC3339),
		Fingerprint: finger,
		CacheValid:  true,
		CachedAt:    time.Now().Format(time.RFC3339),
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("license activated but could not save: %w", err)
	}
	return nil
}

func doActivate(client *http.Client, key, instanceName string) (lsActivateResponse, error) {
	data := url.Values{}
	data.Set("license_key", key)
	data.Set("instance_name", instanceName)

	resp, err := client.PostForm(lsActivateURL, data)
	if err != nil {
		return lsActivateResponse{}, fmt.Errorf("could not reach activation server: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result lsActivateResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return lsActivateResponse{}, fmt.Errorf("invalid response from server: %w", err)
	}
	return result, nil
}

func doDeactivate(client *http.Client, key, instanceID string) error {
	data := url.Values{}
	data.Set("license_key", key)
	data.Set("instance_id", instanceID)

	resp, err := client.PostForm(lsDeactivateURL, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// deactivateLicense desactiva en LS y elimina la licencia local
func deactivateLicense() {
	cfg := loadConfig()
	lic := cfg.License
	if lic.KeyEnc != "" && lic.InstanceID != "" {
		key, err := decryptKey(lic.KeyEnc)
		if err == nil {
			client := &http.Client{Timeout: 8 * time.Second}
			_ = doDeactivate(client, key, lic.InstanceID)
		}
	}
	cfg.License = licenseData{}
	_ = saveConfig(cfg)
}

// handleLicense enruta subcomandos de "ksw license"
func handleLicense() {
	args := os.Args[2:]

	if len(args) == 0 {
		printLicenseStatus()
		return
	}

	switch args[0] {
	case "activate":
		key := ""
		if len(args) >= 2 {
			key = strings.Join(args[1:], "")
		} else {
			fmt.Print("  Enter license key: ")
			key = readMasked()
			fmt.Println()
		}
		if key == "" {
			fmt.Fprintln(os.Stderr, warnStyle.Render("✗")+" No license key provided.")
			os.Exit(1)
		}
		fmt.Print(dimStyle.Render("  Activating license..."))
		if err := activateLicense(key); err != nil {
			fmt.Println()
			fmt.Fprintln(os.Stderr, warnStyle.Render("✗ ")+err.Error())
			os.Exit(1)
		}
		cfg := loadConfig()
		fmt.Println("\r\033[K" + successStyle.Render("✔") + " License activated successfully!")
		if cfg.License.Email != "" {
			fmt.Println("  " + dimStyle.Render("Registered to: "+cfg.License.Email))
		}
		fmt.Println()
		fmt.Println("  " + successStyle.Render("★") + " All premium features are now unlocked.")
		fmt.Println("  " + dimStyle.Render("Get started: ksw aws sso config"))

	case "status":
		printLicenseStatus()

	case "buy":
		buyURL := "https://ksw.lemonsqueezy.com/checkout/buy/5b89e2bc-9b58-4343-84d3-2dcbf22d67a1"
		fmt.Println("  " + dimStyle.Render("Opening: "+buyURL))
		_ = exec.Command("open", buyURL).Start()

	case "deactivate", "desactivate":
		fmt.Print(dimStyle.Render("  Deactivating..."))
		deactivateLicense()
		fmt.Println("\r\033[K" + successStyle.Render("✔") + " License removed from this machine.")

	default:
		fmt.Fprintf(os.Stderr, warnStyle.Render("✗")+" Unknown subcommand: %s\n", args[0])
		os.Exit(1)
	}
}

func printLicenseStatus() {
	fmt.Println(logoStyle.Render("⎈ ksw license"))
	fmt.Println()
	cfg := loadConfig()
	lic := cfg.License
	if lic.KeyEnc == "" {
		fmt.Println("  " + warnStyle.Render("✗") + " No license activated on this machine.")
		fmt.Println()
		fmt.Println("  Commands:")
		fmt.Println("    ksw license activate         Activate interactively (key hidden)")
		fmt.Println("    ksw license activate <key>   Activate with key as argument")
		fmt.Println("    ksw license buy              Open checkout in browser")
		return
	}
	key, err := decryptKey(lic.KeyEnc)
	valid := err == nil && validateWithLS(key, lic.InstanceID)
	if valid {
		fmt.Println("  " + successStyle.Render("✔") + " License active")
	} else {
		fmt.Println("  " + warnStyle.Render("✗") + " License invalid or revoked")
	}
	if lic.Email != "" {
		fmt.Println("  " + dimStyle.Render("Email:       " + lic.Email))
	}
	if lic.ActivatedAt != "" {
		fmt.Println("  " + dimStyle.Render("Activated:   " + lic.ActivatedAt))
	}
	if lic.CachedAt != "" {
		fmt.Println("  " + dimStyle.Render("Last check:  " + lic.CachedAt))
	}
	fmt.Println()
	fmt.Println("  Commands:")
	fmt.Println("    ksw license deactivate       Remove license from this machine (to move to another)")
	fmt.Println("    ksw license activate <key>   Re-activate on this machine")
}
