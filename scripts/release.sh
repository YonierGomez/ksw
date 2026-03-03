#!/usr/bin/env bash
# release.sh — automatiza el flujo completo de release para ksw
# Uso: ./scripts/release.sh <version> "<descripción>"
# Ejemplo: ./scripts/release.sh 1.3.4 "fix: corregir bug en alias"

set -e

VERSION="$1"
DESCRIPTION="$2"
HOMEBREW_TAP_DIR="/opt/homebrew/Library/Taps/yoniergomez/homebrew-ksw"

if [[ -z "$VERSION" || -z "$DESCRIPTION" ]]; then
  echo "Uso: $0 <version> \"<descripción>\""
  echo "Ejemplo: $0 1.3.4 \"fix: corregir bug en alias\""
  exit 1
fi

TAG="v$VERSION"

echo "🔍 Validando entorno antes de release $TAG..."

# ── VALIDACIONES PREVIAS ───────────────────────────────

# 1. gh CLI disponible y autenticado
if ! gh auth status &>/dev/null; then
  echo "❌ gh CLI no autenticado. Ejecuta: gh auth login"
  exit 1
fi
echo "✔ gh CLI autenticado"

# 2. Estar en main actualizado
git checkout main
git pull origin main
echo "✔ main actualizado"

# 3. Working tree limpio
if [[ -n "$(git status --porcelain)" ]]; then
  echo "❌ Hay cambios sin commitear en el working tree:"
  git status --short
  exit 1
fi
echo "✔ Working tree limpio"

# 4. El tag no existe ya
if git tag | grep -q "^${TAG}$"; then
  echo "❌ El tag $TAG ya existe localmente. Bórralo primero:"
  echo "   git tag -d $TAG && git push origin :refs/tags/$TAG"
  exit 1
fi
if git ls-remote --tags origin | grep -q "refs/tags/${TAG}$"; then
  echo "❌ El tag $TAG ya existe en origin. Bórralo primero:"
  echo "   git push origin :refs/tags/$TAG"
  exit 1
fi
echo "✔ Tag $TAG no existe"

# 5. const version en main.go coincide con VERSION
CURRENT_VERSION=$(grep 'const version' main.go | grep -o '"[^"]*"' | tr -d '"')
if [[ "$CURRENT_VERSION" != "$VERSION" ]]; then
  echo "❌ const version en main.go es '$CURRENT_VERSION', esperaba '$VERSION'"
  echo "   Actualiza main.go primero con: ./scripts/release.sh $VERSION \"...\""
  echo "   O edita main.go manualmente y haz PR antes de correr este script."
  exit 1
fi
echo "✔ const version=$CURRENT_VERSION coincide con $TAG"

# 6. Archivos clave existen
for f in main.go go.mod Formula/ksw.rb scripts/release.sh; do
  if [[ ! -f "$f" ]]; then
    echo "❌ Archivo requerido no encontrado: $f"
    exit 1
  fi
done
echo "✔ Archivos clave presentes"

# 7. Homebrew tap dir existe
if [[ ! -d "$HOMEBREW_TAP_DIR" ]]; then
  echo "❌ Homebrew tap no encontrado en $HOMEBREW_TAP_DIR"
  echo "   Ejecuta: brew tap yoniergomez/ksw"
  exit 1
fi
echo "✔ Homebrew tap presente"

echo ""
echo "✅ Todas las validaciones pasaron. Iniciando release $TAG..."
echo ""

# ── RELEASE ────────────────────────────────────────────

# 8. Crear y pushear el tag
echo "🏷️  Creando tag $TAG..."
git tag "$TAG"
git push origin "$TAG"

# 9. Esperar tarball
echo "⏳ Esperando tarball en GitHub..."
TARBALL_URL="https://github.com/YonierGomez/ksw/archive/refs/tags/${TAG}.tar.gz"
for i in {1..20}; do
  STATUS=$(curl -sI "$TARBALL_URL" | head -1 | awk '{print $2}')
  if [[ "$STATUS" == "302" || "$STATUS" == "200" ]]; then
    echo "✔ Tarball disponible"
    break
  fi
  if [[ $i -eq 20 ]]; then
    echo "❌ Tarball no disponible después de 100s. Abortando."
    exit 1
  fi
  echo "  Intento $i/20..."
  sleep 5
done

# 10. Calcular sha256
echo "🔐 Calculando sha256..."
SHA256=$(curl -sL "$TARBALL_URL" | shasum -a 256 | awk '{print $1}')
echo "   sha256: $SHA256"

# 11. Verificar que el tarball contiene la versión correcta
TARBALL_VERSION=$(curl -sL "$TARBALL_URL" | tar xz -O --include="*/main.go" 2>/dev/null | grep 'const version' | grep -o '"[^"]*"' | tr -d '"')
if [[ "$TARBALL_VERSION" != "$VERSION" ]]; then
  echo "❌ El tarball contiene const version='$TARBALL_VERSION', esperaba '$VERSION'"
  echo "   Borra el tag y vuelve a intentar:"
  echo "   git tag -d $TAG && git push origin :refs/tags/$TAG"
  exit 1
fi
echo "✔ Tarball verificado — const version=$TARBALL_VERSION"

BRANCH="chore/bump-$TAG"

# 12. Actualizar versión en index.html
echo "🌐 Actualizando versión en index.html..."
sed -i '' "s/\"softwareVersion\": \"[0-9]*\.[0-9]*\.[0-9]*\"/\"softwareVersion\": \"$VERSION\"/g" index.html
sed -i '' "s/⎈ v[0-9]*\.[0-9]*\.[0-9]* · AI-Powered/⎈ v$VERSION · AI-Powered/g" index.html
sed -i '' "s/AI-Powered Kubernetes context switcher · v[0-9]*\.[0-9]*\.[0-9]*/AI-Powered Kubernetes context switcher · v$VERSION/g" index.html

# Verificar que index.html quedó bien
HTML_VERSION=$(grep 'softwareVersion' index.html | grep -o '[0-9]*\.[0-9]*\.[0-9]*')
if [[ "$HTML_VERSION" != "$VERSION" ]]; then
  echo "❌ index.html no se actualizó correctamente"
  exit 1
fi
echo "✔ index.html actualizado a $VERSION"

# 13. Actualizar Formula/ksw.rb en repo principal
echo "📝 Actualizando Formula/ksw.rb en ksw..."
git checkout -b "$BRANCH"
sed -i '' "s|refs/tags/v[0-9]*\.[0-9]*\.[0-9]*.tar.gz|refs/tags/${TAG}.tar.gz|g" Formula/ksw.rb
sed -i '' "s|sha256 \"[a-f0-9]*\"|sha256 \"$SHA256\"|g" Formula/ksw.rb
git add Formula/ksw.rb index.html
git commit -m "chore: bump formula to $TAG"
git push origin "$BRANCH"

gh pr create \
  --repo YonierGomez/ksw \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump formula to $TAG" \
  --body "$DESCRIPTION"

gh pr merge --repo YonierGomez/ksw --squash "$BRANCH"

# 13. Esperar a que el PR quede mergeado en main
echo "⏳ Esperando merge en main..."
for i in {1..15}; do
  git checkout main && git pull origin main
  FORMULA_VERSION=$(grep 'refs/tags' Formula/ksw.rb | grep -o 'v[0-9]*\.[0-9]*\.[0-9]*')
  if [[ "$FORMULA_VERSION" == "$TAG" ]]; then
    echo "✔ Formula en main actualizada a $TAG"
    break
  fi
  if [[ $i -eq 15 ]]; then
    echo "❌ Formula en main no se actualizó después de esperar. Revisa el PR manualmente."
    exit 1
  fi
  echo "  Esperando... ($i/15)"
  sleep 5
done

# 14. Actualizar formula en homebrew-ksw
echo "🍺 Actualizando homebrew-ksw..."
git -C "$HOMEBREW_TAP_DIR" checkout main
git -C "$HOMEBREW_TAP_DIR" pull origin main
git -C "$HOMEBREW_TAP_DIR" checkout -b "$BRANCH"
sed -i '' "s|refs/tags/v[0-9]*\.[0-9]*\.[0-9]*.tar.gz|refs/tags/${TAG}.tar.gz|g" "$HOMEBREW_TAP_DIR/Formula/ksw.rb"
sed -i '' "s|sha256 \"[a-f0-9]*\"|sha256 \"$SHA256\"|g" "$HOMEBREW_TAP_DIR/Formula/ksw.rb"
git -C "$HOMEBREW_TAP_DIR" add Formula/ksw.rb
git -C "$HOMEBREW_TAP_DIR" commit -m "chore: bump formula to $TAG"
git -C "$HOMEBREW_TAP_DIR" push origin "$BRANCH"

gh pr create \
  --repo YonierGomez/homebrew-ksw \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump formula to $TAG" \
  --body "$DESCRIPTION"

gh pr merge --repo YonierGomez/homebrew-ksw --squash "$BRANCH"

# 15. Esperar merge en homebrew-ksw
echo "⏳ Esperando merge en homebrew-ksw..."
for i in {1..15}; do
  git -C "$HOMEBREW_TAP_DIR" checkout main && git -C "$HOMEBREW_TAP_DIR" pull origin main
  TAP_VERSION=$(grep 'refs/tags' "$HOMEBREW_TAP_DIR/Formula/ksw.rb" | grep -o 'v[0-9]*\.[0-9]*\.[0-9]*')
  if [[ "$TAP_VERSION" == "$TAG" ]]; then
    echo "✔ homebrew-ksw actualizado a $TAG"
    break
  fi
  if [[ $i -eq 15 ]]; then
    echo "❌ homebrew-ksw no se actualizó. Revisa el PR manualmente."
    exit 1
  fi
  echo "  Esperando... ($i/15)"
  sleep 5
done

# 16. brew upgrade y verificación final
echo "🔄 Actualizando brew..."
brew update && brew upgrade ksw

INSTALLED_VERSION=$(ksw -v | grep -o '[0-9]*\.[0-9]*\.[0-9]*')
if [[ "$INSTALLED_VERSION" != "$VERSION" ]]; then
  echo "❌ ksw -v reporta '$INSTALLED_VERSION', esperaba '$VERSION'"
  exit 1
fi

echo ""
echo "✅ Release $TAG completado y verificado"
echo "   - const version=$VERSION ✔"
echo "   - Tag $TAG ✔"
echo "   - Formula en ksw ✔"
echo "   - Formula en homebrew-ksw ✔"
echo "   - ksw -v = $INSTALLED_VERSION ✔"
