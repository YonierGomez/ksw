# Release Workflow

## Flujo para subir cambios

### 1. Cambios normales (sin release)
Siempre crear rama desde `main`, nunca pushear directo a `main`.

```bash
git checkout main && git pull origin main
git checkout -b <tipo>/<descripcion>
# hacer cambios
git add . && git commit -m "<tipo>: <descripcion>"
git push origin <tipo>/<descripcion>
gh pr create --repo YonierGomez/ksw --base main --head <rama> --title "<titulo>" --body "<descripcion>"
gh pr merge --repo YonierGomez/ksw --squash --auto <rama>
```

Tipos de rama: `feat/`, `fix/`, `chore/`, `docs/`

### 2. Release nuevo (con versión)
Usar el script automatizado que hace todo:

```bash
./scripts/release.sh <version> "<descripcion>"
# Ejemplo:
./scripts/release.sh 1.3.2 "fix: corregir bug en alias"
```

El script hace automáticamente:
- Tag `vX.Y.Z` en `main`
- Calcula sha256 del tarball
- Actualiza `Formula/ksw.rb` en `YonierGomez/ksw`
- Crea y mergea PR en `YonierGomez/ksw`
- Actualiza formula en `YonierGomez/homebrew-ksw`
- Crea y mergea PR en `YonierGomez/homebrew-ksw`
- Ejecuta `brew upgrade ksw` local

### Repos involucrados
- Código fuente: `git@github.com:YonierGomez/ksw.git`
- Homebrew tap: `git@github.com:YonierGomez/homebrew-ksw.git` (en `/opt/homebrew/Library/Taps/yoniergomez/homebrew-ksw`)
- Landing page: `https://yoniergomez.github.io/ksw/` (se despliega automáticamente desde `main`)

### Después de cada merge
Siempre volver a `main`, borrar la rama local y quedar limpio:

```bash
git checkout main && git pull origin main
git branch -D <rama-mergeada>
```

### Reglas importantes
- Git remote usa SSH (`git@github.com:...`), nunca HTTPS
- `main` tiene protección — no se puede pushear directo, siempre por PR
- No incluir nombres reales de clusters (nequi, sufi, clientes, apic, etc.) en landing ni demos
- No incluir `rm -f ~/.ksw.json` en tapes de VHS
- `gh` CLI está instalado y autenticado con SSH como `YonierGomez`
