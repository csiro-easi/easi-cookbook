#!/bin/bash
#
# Create a lightweight virtual environment on EASI docker images using uv.
# Dry-runs package resolution, diffs against system site-packages,
# and installs only new packages.
#
# ---------------------------------------------------------------------------
# USAGE (interactive shell):
#   PACKAGES='pkg1 pkg2' bash easi-venv-setup.sh
#   Then activate manually: source ~/venvs/myvenv/bin/activate
#
# USAGE (from Python / Dask worker):
#   import subprocess, os
#   subprocess.run(["bash", "/path/to/easi-venv-setup.sh"],
#                  env={**os.environ, "PACKAGES": "pkg1 pkg2", "VENV_NAME": "myenv"},
#                  check=True)
# ---------------------------------------------------------------------------

# set -euo pipefail

# =============================================================================
# HELP
# =============================================================================
show_help() {
    cat <<'EOF'
easi-venv-setup.sh — Create a lightweight virtual environment on EASI images using uv.

USAGE:
  PACKAGES='pkg1 pkg2' bash easi-venv-setup.sh
  PACKAGES='pkg1 pkg2' VENV_NAME=myenv bash easi-venv-setup.sh

REQUIRED (at least one):
  PACKAGES        Space-separated list of packages to install (pip specifiers)
  EDITABLE_PKG    Path to editable install (e.g. ".")

OPTIONAL (env vars):
  VENV_BASE       Base directory for venvs         [default: $HOME/venvs]
  VENV_NAME       Name of the virtual environment  [default: myvenv]
  VENV_DISP       Jupyter kernel display name      [default: My Venv]
  CONSTRAINTS     Space-separated constraint files [default: /conf/constraints.txt]
                  Contains "pkg>=version" lines to constrain package versions
  OVERRIDES       Space-separated pip overrides files [default: none]
                  Contains "pkg>=version" lines to override constraints (see uv docs)
  INSTALL_KERNEL  Register as Jupyter kernel       [default: false]
  TORCH_BACKEND   PyTorch index for UV to use      [default: cpu]
  VERBOSE         Echo install commands before running [default: false]

EXAMPLES:
  PACKAGES='torch torchvision' VENV_NAME=myenv INSTALL_KERNEL=true bash easi-venv-setup.sh
  EDITABLE_PKG='.' VENV_NAME=myenv bash easi-venv-setup.sh
  bash easi-venv-setup.sh --help
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    show_help
    exit 0
fi

if [ -z "${PACKAGES:-}" ] && [ -z "${EDITABLE_PKG:-}" ]; then
    echo "easi-venv-setup.sh: At least one of PACKAGES or EDITABLE_PKG must be set."
    echo ""
    echo "  Usage: PACKAGES='pkg1 pkg2' bash easi-venv-setup.sh"
    echo "         EDITABLE_PKG='.' bash easi-venv-setup.sh"
    echo "  Run with --help for full options."
    exit 1
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
TORCH_BACKEND="${TORCH_BACKEND:-cpu}"

# System overrides file, if it exists in the given image
SYS_OVERRIDES="/conf/pip-overrides.txt"

# System no-binary file, if it exists in the given image
NOBINARY="/conf/no-binary.txt"

# =============================================================================
# DERIVED VALUES
# =============================================================================
PYVERSION=$(python3 --version | awk '{print tolower($1$2)}' | sed 's/\.[0-9]*$//')
VENV_PYTHON="$VENV_BASE/$VENV_NAME/bin/python"

# System uv options
UV_OPTIONS=(--system-certs --no-build-isolation)
UV_OPTIONS+=(--torch-backend="$TORCH_BACKEND")
UV_OPTIONS+=(--color=never)  # disable color in output for easier parsing

# =============================================================================
# CREATE VENV
# The --system-site-packages flag will provide the venv with access to the system site packages directory at runtime.
# uv will not take system site packages into account when running commands like uv pip list or uv pip install.
# =============================================================================
if [ ! -d "$VENV_BASE/$VENV_NAME" ]; then
    echo "Creating virtual environment \"$VENV_NAME\""
    uv venv --system-site-packages "$VENV_BASE/$VENV_NAME"
    realpath /env/lib/$PYVERSION/site-packages > "$VENV_BASE/$VENV_NAME/lib/$PYVERSION/site-packages/base_venv.pth"
else
    echo "Virtual environment \"$VENV_NAME\" already exists"
fi

# =============================================================================
# RESOLVE & INSTALL ONLY NEW PACKAGES
# =============================================================================
echo "Resolving dependencies..."

# List of currently installed packages in system site-packages (lowercased, with underscores replaced by dashes for comparison)
FROZEN=$(uv pip freeze --python /env --color=never 2>/dev/null | tr '[:upper:]' '[:lower:]' | tr '_' '-')

# Build dry-run command
DRY_RUN_CMD=(uv pip install --dry-run --python "$VENV_PYTHON" "${UV_OPTIONS[@]}")

# Add system constraints and overrides if they exist
# SYS_OVERRIDES contains "--override somefile.txt" lines, so we can xargs it into the command
[ -f "$NOBINARY" ] && DRY_RUN_CMD+=(-c "$NOBINARY")
[ -f "$SYS_OVERRIDES" ] && DRY_RUN_CMD+=($(xargs < "${SYS_OVERRIDES}"))

# Add user constraints
for cf in $CONSTRAINTS; do
    [ -f "$cf" ] && DRY_RUN_CMD+=(-c "$cf")
done
# Add user overrides
for cf in $OVERRIDES; do
    [ -f "$cf" ] && DRY_RUN_CMD+=(--override "$cf")
done

# Add packages to install
[ -n "$PACKAGES" ] && DRY_RUN_CMD+=($PACKAGES)
[ -n "$EDITABLE_PKG" ] && DRY_RUN_CMD+=("$EDITABLE_PKG")

echo "+ ${DRY_RUN_CMD[*]}"

# Capture dry-run output (uv writes the package list to stderr; 2>&1 merges it).
# Capturing to a variable lets us check the exit code - a non-zero exit inside a
# process substitution is silently ignored in bash without set -e.
DRY_RUN_OUTPUT=$("${DRY_RUN_CMD[@]}" 2>&1)
DRY_RUN_EXIT=$?
if [ $DRY_RUN_EXIT -ne 0 ]; then
    echo "ERROR: dependency resolution failed (exit $DRY_RUN_EXIT):"
    echo "$DRY_RUN_OUTPUT"
    exit 1
fi
[ "$VERBOSE" = "true" ] && echo "$DRY_RUN_OUTPUT"

# Parse dry-run output against frozen system packages to find new packages to install.
# - dry run outputs lines like "name==version"
# - we normalize names (lowercase, underscore→dash) for comparison
# - SKIP only if the exact name==version is already in the system
# - if uv resolves a NEWER version than the system has, install it into the venv (shadows system)
# - users control versions via CONSTRAINTS / OVERRIDES; /conf/pip-overrides.txt is always applied
EXTRA=()
echo "Comparing against system packages..."
while IFS= read -r pkg; do
    [ -z "$pkg" ] && continue
    pkgnorm=$(echo "$pkg" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
    pkgname=$(echo "$pkgnorm" | cut -d'=' -f1)
    sys_entry=$(echo "$FROZEN" | grep "^${pkgname}==")

    if echo "$FROZEN" | grep -q "^${pkgnorm}$"; then
        echo "  skip    $pkg (exact version already in system)"
    elif [ -n "$sys_entry" ]; then
        echo "  upgrade $pkg (system has $sys_entry)"
        EXTRA+=("$pkg")
    else
        echo "  new     $pkg"
        EXTRA+=("$pkg")
    fi
done < <(
    echo "$DRY_RUN_OUTPUT" \
    | grep -E '^\s+\+\s+[a-zA-Z0-9_-]+==' \
    | awk '{print $2}'
)

# Install only the new packages, if any
if [ ${#EXTRA[@]} -eq 0 ]; then
    echo "All dependencies already present; nothing to install."
else
    echo "Installing new packages..."
    INSTALL_CMD=(uv pip install --no-deps --python "$VENV_PYTHON" "${UV_OPTIONS[@]}" "${EXTRA[@]}")
    # [ "$VERBOSE" = "true" ] && echo "+ ${INSTALL_CMD[*]}"
    "${INSTALL_CMD[@]}"
fi

# =============================================================================
# OPTIONAL: EDITABLE LOCAL PACKAGE
# All dependencies and constraints should be handled in the above steps
# =============================================================================
if [ -n "$EDITABLE_PKG" ]; then
    INSTALL_CMD=(uv pip install --no-deps --python "$VENV_PYTHON" -e "$EDITABLE_PKG")
    # [ "$VERBOSE" = "true" ] && echo "+ ${INSTALL_CMD[*]}"
    "${INSTALL_CMD[@]}"
fi

# =============================================================================
# OPTIONAL: JUPYTER KERNEL
# =============================================================================
if [ "$INSTALL_KERNEL" = "true" ]; then
    if jupyter kernelspec list 2>/dev/null | grep -q " $VENV_NAME "; then
        echo "Kernel \"$VENV_NAME\" already registered"
    else
        "$VENV_PYTHON" -m ipykernel install --user --name="$VENV_NAME" --display-name "$VENV_DISP"
        echo "Installed kernel \"$VENV_NAME\""
    fi
fi

echo ""
echo "Done. To activate this environment in your shell:"
echo "  source \"$VENV_BASE/$VENV_NAME/bin/activate\""
