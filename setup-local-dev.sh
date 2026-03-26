#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC} $1"; }
fail() { echo -e "${RED}[MISSING]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "  Legal Chatbot - Environment Setup"
echo "============================================"
echo ""

# -----------------------------------------------
# 1. Docker (required for docker-compose)
# -----------------------------------------------
echo "--- Checking Docker ---"
if command -v docker &>/dev/null; then
  ok "Docker $(docker --version)"
else
  fail "Docker not found. Install Docker Desktop first: https://docker.com/products/docker-desktop"
  exit 1
fi

# -----------------------------------------------
# 2. Python
# -----------------------------------------------
echo ""
echo "--- Checking Python ---"
PYTHON_BIN=""
if command -v python3.11 &>/dev/null; then
  PYTHON_BIN="python3.11"
elif command -v python3 &>/dev/null; then
  PYTHON_BIN="python3"
fi

if [ -n "$PYTHON_BIN" ]; then
  ok "$($PYTHON_BIN --version)"
else
  fail "Python not found. Run: brew install python@3.11"
  exit 1
fi

# -----------------------------------------------
# 3. Java 17 (required by Spark)
# -----------------------------------------------
echo ""
echo "--- Checking Java ---"
if java -version 2>&1 | grep -q "version"; then
  ok "Java $(java -version 2>&1 | head -1)"
else
  fail "Java not found. Run: brew install openjdk@17"
  exit 1
fi

# -----------------------------------------------
# 4. Node.js
# -----------------------------------------------
echo ""
echo "--- Checking Node.js ---"
if command -v node &>/dev/null; then
  ok "Node.js $(node --version)"
else
  fail "Node.js not found. Run: brew install node@20"
  exit 1
fi

# -----------------------------------------------
# 5. Python venv + dependencies
# -----------------------------------------------
echo ""
echo "--- Setting up Python venv ---"
VENV_DIR="${SCRIPT_DIR}/.venv"

if [ ! -d "$VENV_DIR" ]; then
  $PYTHON_BIN -m venv "$VENV_DIR"
  ok "Created .venv/"
else
  ok ".venv/ already exists"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r "${SCRIPT_DIR}/requirements.txt" -q
pip install -r "${SCRIPT_DIR}/requirements-spark.txt" -q
ok "Python dependencies installed"
deactivate

# -----------------------------------------------
# 6. Frontend dependencies
# -----------------------------------------------
echo ""
echo "--- Setting up frontend ---"
cd "${SCRIPT_DIR}/frontend"
npm install --silent 2>/dev/null
ok "Frontend dependencies installed"
cd "$SCRIPT_DIR"

# -----------------------------------------------
# 7. .env file
# -----------------------------------------------
echo ""
echo "--- Checking .env ---"
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
  cp "${SCRIPT_DIR}/.env.example" "${SCRIPT_DIR}/.env"
  ok "Created .env from .env.example"
else
  ok ".env already exists"
fi

# -----------------------------------------------
# Done
# -----------------------------------------------
echo ""
echo "============================================"
echo "  Done! Next steps:"
echo "============================================"
echo ""
echo "  1. Edit .env         ->  set OPENAI_API_KEY"
echo "  2. source .venv/bin/activate"
echo "  3. docker-compose up -d"
echo ""
