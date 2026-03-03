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

echo "🚀 Iniciando release $TAG..."

# ── VALIDACIONES PREVIAS ───────────────────────────────

# 1. Estar en main actualizado
git checkout main
git pull origin main

# 2. Verificar que el tag no existe ya
if git tag | grep -q "^${TAG}$"; then
  echo "❌ El tag $TAG ya existe. Aborta."
  exit 1
fi

# 3. Verificar que const version en main.go coincide con VERSION
CURRENT_VERSION=$(grep 'const version' main.go | grep -o '"[^"]*"' | tr -d '"')
if [[ "$CURRENT_VERSION" != "$VERSION" ]]; then
  echo "❌ const version en main.go es '$CURRENT_VERSION', esperaba '$VERSION'"
  echo "   Actualiza main.go primero o deja que el script lo haga automáticamente."
  echo ""
  read -p "   ¿Actualizar main.go a $VERSION automáticamente? [s/N]: " CONFIRM
  if [[ "$CONFIRM" != "s" && "$CONFIRM" != "S" ]]; then
    echo "Abortado."
    exit 1
  fi

  # Bump version en main.go via PR
  VERSION_BRANCH="chore/bump-version-$TAG"
  git checkout -b "$VERSION_BRANCH"
  sed -i '' "s|const version = \"[0-9]*\.[0-9]*\.[0-9]*\"|const version = \"$VERSION\"|g" main.go
  git add main.go
  git commit -m "chore: bump version constant to $TAG"
  git push origin "$VERSION_BRANCH"

  gh pr create \
    --repo YonierGomez/ksw \
    --base main \
    --head "$VERSION_BRANCH" \
    --title "chore: bump version constant to $TAG" \
    --body "$DESCRIPTION"

  gh pr merge --repo YonierGomez/ksw --squash "$VERSION_BRANCH"

  git checkout main
  git pull origin main
fi

# 4. Verificar una vez más que todo coincide antes de continuar
CURRENT_VERSION=$(grep 'const version' main.go | grep -o '"[^"]*"' | tr -d '"')
if [[ "$CURRENT_VERSION" != "$VERSION" ]]; then
  echo "❌ const version sigue siendo '$CURRENT_VERSION'. Algo falló. Abortando."
  exit 1
fi

echo "✔ Validaciones OK — const version=$CURRENT_VERSION, tag=$TAG no existe"

# ── RELEASE ────────────────────────────────────────────

# 5. Crear y pushear el tag
echo "🏷️  Creando tag $TAG..."
git tag "$TAG"
git push origin "$TAG"

# 6. Esperar tarball
echo "⏳ Esperando tarball en GitHub..."
TARBALL_URL="https://github.com/YonierGomez/ksw/archive/refs/tags/${TAG}.tar.gz"
for i in {1..20}; do
  STATUS=$(curl -sI "$TARBALL_URL" | head -1 | awk '{print $2}')
  if [[ "$STATUS" == "302" || "$STATUS" == "200" ]]; then
    echo "✔ Tarball disponible"
    break
  fi
  echo "  Intento $i/20..."
  sleep 5
done

# 7. Calcular sha256
echo "🔐 Calculando sha256..."
SHA256=$(curl -sL "$TARBALL_URL" | shasum -a 256 | awk '{print $1}')
echo "   sha256: $SHA256"

# 8. Verificar que el tarball contiene la versión correcta
TARBALL_VERSION=$(curl -sL "$TARBALL_URL" | tar xz -O --include="*/main.go" 2>/dev/null | grep 'const version' | grep -o '"[^"]*"' | tr -d '"')
if [[ "$TARBALL_VERSION" != "$VERSION" ]]; then
  echo "❌ El tarball contiene const version='$TARBALL_VERSION', esperaba '$VERSION'. Abortando."
  echo "   Borra el tag con: git tag -d $TAG && git push origin :refs/tags/$TAG"
  exit 1
fi
echo "✔ Tarball verificado — const version=$TARBALL_VERSION"

BRANCH="chore/bump-$TAG"

# 9. Actualizar Formula/ksw.rb en repo principal
echo "📝 Actualizando Formula/ksw.rb en ksw..."
git checkout -b "$BRANCH"
sed -i '' "s|refs/tags/v[0-9]*\.[0-9]*\.[0-9]*.tar.gz|refs/tags/${TAG}.tar.gz|g" Formula/ksw.rb
sed -i '' "s|sha256 \"[a-f0-9]*\"|sha256 \"$SHA256\"|g" Formula/ksw.rb
git add Formula/ksw.rb
git commit -m "chore: bump formula to $TAG"
git push origin "$BRANCH"

gh pr create \
  --repo YonierGomez/ksw \
  --base main \
  --head "$BRANCH" \
  --title "chore: bump formula to $TAG" \
  --body "$DESCRIPTION"

gh pr merge --repo YonierGomez/ksw --squash "$BRANCH"

# 10. Actualizar formula en homebrew-ksw
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

# 11. Actualizar brew local
echo "🔄 Actualizando brew..."
brew update && brew upgrade ksw

echo ""
echo "✅ Release $TAG completado"
echo "   - const version=$VERSION ✔"
echo "   - Tag $TAG pusheado ✔"
echo "   - Formula actualizada en ksw y homebrew-ksw ✔"
echo "   - brew upgrade ksw ✔"
