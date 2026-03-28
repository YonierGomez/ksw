# GitHub Copilot Instructions — AI Locker

## Git Workflow (OBLIGATORIO)

Antes de realizar **cualquier cambio de código**, siempre seguir este flujo:

1. Estar en `main` actualizado: `git checkout main && git pull origin main`
2. Crear una rama descriptiva: `git checkout -b <tipo>/<descripcion-corta>`
   - Tipos: `feat/`, `fix/`, `chore/`, `docs/`, `refactor/`
3. Hacer los cambios en esa rama
4. Commit y push: `git add . && git commit -m "..." && git push origin <rama>`
5. Crear PR: `gh pr create --title "..." --body "..." --base main`
6. Merge y borrar rama remota: `gh pr merge <num> --squash --delete-branch --admin`
7. Volver a main y limpiar: `git checkout main && git pull && git fetch --prune`

**Nunca hacer commits directamente en `main`.**
