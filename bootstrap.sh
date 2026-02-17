#!/bin/bash
# Bootstrap script - Run once after cloning the repo
# Sets up secret scanning to prevent committing secrets.
# Usage: ./bootstrap.sh

set -e

echo "Bootstrapping atlas-metastore repository..."
echo ""

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    echo ""
    echo "pre-commit is not installed. Installing now..."

    if command -v brew &> /dev/null; then
        brew install pre-commit
    elif command -v pip3 &> /dev/null; then
        pip3 install pre-commit
    else
        echo "Error: Neither brew nor pip3 found."
        echo "Please install pre-commit manually: https://pre-commit.com/#install"
        exit 1
    fi
fi

# Check if gitleaks is installed
if ! command -v gitleaks &> /dev/null; then
    echo ""
    echo "gitleaks is not installed. Installing now..."

    if command -v brew &> /dev/null; then
        brew install gitleaks
    elif [ "$(uname -s)" = "Linux" ]; then
        GITLEAKS_VERSION=$(curl -s https://api.github.com/repos/gitleaks/gitleaks/releases/latest | grep '"tag_name"' | sed 's/.*"v\(.*\)".*/\1/')
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64) ARCH="x64" ;;
            aarch64|arm64) ARCH="arm64" ;;
        esac
        curl -sSfL "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/gitleaks_${GITLEAKS_VERSION}_linux_${ARCH}.tar.gz" | tar -xz -C /tmp
        sudo mv /tmp/gitleaks /usr/local/bin/gitleaks
        echo "gitleaks ${GITLEAKS_VERSION} installed to /usr/local/bin/"
    else
        echo "Error: Could not auto-install gitleaks."
        echo "Please install manually: https://github.com/gitleaks/gitleaks#installing"
        exit 1
    fi
fi

# Install pre-commit hooks into .git/hooks/
echo ""
echo "Installing pre-commit hooks..."
pre-commit install

# Copy post-checkout hook for auto-setup on future clones
if [ -f .githooks/post-checkout ]; then
    cp .githooks/post-checkout .git/hooks/post-checkout
    chmod +x .git/hooks/post-checkout
fi

echo ""
echo "Bootstrap complete! Secret scanning is now active."
echo ""
echo "What's protected:"
echo "  - AWS keys, API keys, JWT tokens"
echo "  - Private keys, database connection strings"
echo "  - Generic secrets and credentials"
echo ""
echo "To test: echo 'AKIAIOSFODNN7EXAMPLE' > test.txt && git add test.txt && git commit -m 'test'"
echo "  (this should be blocked by gitleaks)"
