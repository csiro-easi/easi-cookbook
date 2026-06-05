#!/bin/bash
#
# Create a lightweight virtual environment on EASI docker images using uv.
# Dry-runs package resolution, diffs against system site-packages,
# and installs only new packages.
#
# ---------------------------------------------------------------------------
# USAGE (interactive shell):
#   PACKAGES='pkg1 pkg2' source easi-venv-setup.sh
#
# USAGE (Dask worker via UploadFile plugin):
#   from dask.distributed import Client, UploadFile
#   client = Client(cluster)
#   client.register_plugin(UploadFile("easi-venv-setup.sh"))
#   def setup_worker():
#       import subprocess
#       subprocess.run(["bash", "-c", "PACKAGES='pkg1 pkg2' source /tmp/easi-venv-setup.sh"], check=True)
#   client.run(setup_worker)
# ---------------------------------------------------------------------------

# set -euo pipefail

# =============================================================================
# DETECT IF SOURCED OR EXECUTED
# =============================================================================
_sourced=false
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    _sourced=true
fi

if [ "$_sourced" = false ]; then
    echo "easi-venv-setup.sh: This script should be sourced, not executed directly."
    echo ""
    echo "  Usage: PACKAGES='pkg1 pkg2' source easi-venv-setup.sh"
    echo "  Help:  source easi-venv-setup.sh --help"
    # exit is safe here: we are being executed, not sourced
    exit 1
fi

# =============================================================================
# HELP
# =============================================================================
show_help() {
    cat <<'EOF'
easi-venv-setup.sh — Create a lightweight virtual environment on EASI images using uv.

USAGE:
  PACKAGES='pkg1 pkg2' source easi-venv-setup.sh
  PACKAGES='pkg1 pkg2' VENV_NAME=myenv source easi-venv-setup.sh

REQUIRED (at least one):
  PACKAGES        Space-separated list of packages to install (pip specifiers)
  EDITABLE_PKG    Path to editable install (e.g. ".")

OPTIONAL (env vars):
  VENV_BASE       Base directory for venvs         [default: $HOME/venvs]
  VENV_NAME       Name of the virtual environment  [default: myvenv]
  VENV_DISP       Jupyter kernel display name      [default: My Venv]
  CONSTRAINTS     Space-separated constraint files [default: /conf/constraints.txt]
  OVERRIDES       Space-separated pip overrides files [default: none]
  INSTALL_KERNEL  Register as Jupyter kernel       [default: false]
  TORCH_BACKEND   PyTorch index for UV to use      [default: auto]
  VERBOSE         Echo install commands before running [default: false]

EXAMPLES:
  PACKAGES='torch torchvision' VENV_NAME=myenv INSTALL_KERNEL=true source easi-venv-setup.sh
  EDITABLE_PKG='.' VENV_NAME=myenv source easi-venv-setup.sh
  source easi-venv-setup.sh --help
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    show_help
    return 0
fi

if [ -z "${PACKAGES:-}" ] && [ -z "${EDITABLE_PKG:-}" ]; then
    echo "easi-venv-setup.sh: At least one of PACKAGES or EDITABLE_PKG must be set."
    echo ""
    echo "  Usage: PACKAGES='pkg1 pkg2' source easi-venv-setup.sh"
    echo "         EDITABLE_PKG='.' source easi-venv-setup.sh"
    echo "  Run with --help for full options."
    return 1
fi

# =============================================================================
# PARAMETERS — edit these or override via environment variables
# =============================================================================
VENV_BASE="${VENV_BASE:-$HOME/venvs}"
VENV_NAME="${VENV_NAME:-myvenv}"
VENV_DISP="${VENV_DISP:-My Venv}"

# Space-separated list of constraint files
CONSTRAINTS="${CONSTRAINTS:-/conf/constraints.txt}"

# Optional: editable install of a local project path (e.g. "." or "./mylib")
EDITABLE_PKG="${EDITABLE_PKG:-}"

# Optional: space-separated list of user overrides files
OVERRIDES="${OVERRIDES:-}"

# Whether to register as a Jupyter kernel (disable on workers)
INSTALL_KERNEL="${INSTALL_KERNEL:-false}"

# Verbose/debug: echo uv install commands before running
VERBOSE="${VERBOSE:-false}"

# PyTorch index for UV to use (e.g. "cpu", "cu118", "auto")
TORCH_BACKEND="${TORCH_BACKEND:-auto}"

# System overrides file, if it exists in the given image
SYS_OVERRIDES="/opt/pip-overrides.txt"

# System no-binary file, if it exists in the given image
NOBINARY="/conf/no-binary.txt"

# =============================================================================
# DERIVED VALUES
# =============================================================================
PYVERSION=$(python3 --version | awk '{print tolower($1$2)}' | sed 's/\.[0-9]*$//')

# System uv options
UV_OPTIONS=(--system-certs --no-build-isolation)
UV_OPTIONS+=(--torch-backend "$TORCH_BACKEND")

# =============================================================================
# WARN IF ALREADY INSIDE A DIFFERENT VENV
# =============================================================================
if [ -n "${VIRTUAL_ENV:-}" ]; then
    _target_venv="$(realpath "$VENV_BASE/$VENV_NAME" 2>/dev/null || echo "$VENV_BASE/$VENV_NAME")"
    _active_venv="$(realpath "$VIRTUAL_ENV" 2>/dev/null || echo "$VIRTUAL_ENV")"
    if [ "$_active_venv" != "$_target_venv" ]; then
        echo "Warning: already inside a virtual environment: $VIRTUAL_ENV"
        echo "         Switching to \"$VENV_NAME\"..."
    fi
fi

# =============================================================================
# CREATE VENV
# =============================================================================
if [ ! -d "$VENV_BASE/$VENV_NAME" ]; then
    echo "Creating virtual environment \"$VENV_NAME\""
    uv venv --system-site-packages "$VENV_BASE/$VENV_NAME"
    realpath /env/lib/$PYVERSION/site-packages > "$VENV_BASE/$VENV_NAME/lib/$PYVERSION/site-packages/base_venv.pth"
else
    echo "Virtual environment \"$VENV_NAME\" already exists"
fi

source "$VENV_BASE/$VENV_NAME/bin/activate"

# =============================================================================
# RESOLVE & INSTALL ONLY NEW PACKAGES
# =============================================================================
echo "Resolving dependencies..."
FROZEN=$(pip freeze 2>/dev/null | tr '[:upper:]' '[:lower:]' | tr '_' '-')

# Build dry-run command
DRY_RUN_CMD=(uv pip install --dry-run "${UV_OPTIONS[@]}")

# Add system constraints and overrides if they exist
[ -f "$NOBINARY" ] && DRY_RUN_CMD+=(-c "$NOBINARY")
[ -f "$SYS_OVERRIDES" ] && DRY_RUN_CMD+=($(xargs < "${SYS_OVERRIDES}"))

# Add user constraints and overrides
for cf in $CONSTRAINTS $OVERRIDES; do
    [ -f "$cf" ] && DRY_RUN_CMD+=(-c "$cf")
done

# Add packages to install
[ -n "$PACKAGES" ] && DRY_RUN_CMD+=($PACKAGES)
[ -n "$EDITABLE_PKG" ] && DRY_RUN_CMD+=("$EDITABLE_PKG")

[ "$VERBOSE" = "true" ] && echo "+ ${DRY_RUN_CMD[*]}"

EXTRA=()
while IFS= read -r pkg; do
    [ -z "$pkg" ] && continue
    pkgname=$(echo "$pkg" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | cut -d'=' -f1)
    if ! echo "$FROZEN" | grep -q "^${pkgname}=="; then
        EXTRA+=("$pkg")
    fi
done < <(
    "${DRY_RUN_CMD[@]}" 2>&1 \
    | grep -E '^\s+\+\s+[a-zA-Z0-9_-]+==' \
    | awk '{print $2}'
)

if [ ${#EXTRA[@]} -eq 0 ]; then
    echo "All dependencies already present; nothing to install."
else
    echo "Installing ${#EXTRA[@]} new packages into \"$VENV_NAME\":"
    printf '  %s\n' "${EXTRA[@]}"
    INSTALL_CMD=(uv pip install --no-deps "${UV_OPTIONS[@]}" "${EXTRA[@]}")
    [ "$VERBOSE" = "true" ] && echo "+ ${INSTALL_CMD[*]}"
    "${INSTALL_CMD[@]}"
fi

# =============================================================================
# OPTIONAL: EDITABLE LOCAL PACKAGE
# All dependencies and constraints should be handled in the above steps
# =============================================================================
if [ -n "$EDITABLE_PKG" ]; then
    INSTALL_CMD=(uv pip install --no-deps -e "$EDITABLE_PKG")
    [ "$VERBOSE" = "true" ] && echo "+ ${INSTALL_CMD[*]}"
    "${INSTALL_CMD[@]}"
fi

# =============================================================================
# OPTIONAL: JUPYTER KERNEL
# =============================================================================
if [ "$INSTALL_KERNEL" = "true" ]; then
    if jupyter kernelspec list 2>/dev/null | grep -q " $VENV_NAME "; then
        echo "Kernel \"$VENV_NAME\" already registered"
    else
        python -m ipykernel install --user --name="$VENV_NAME" --display-name "$VENV_DISP"
        echo "Installed kernel \"$VENV_NAME\""
    fi
fi
