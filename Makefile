.PHONY: help publish check test clean tag release release-patch release-minor release-major \
        dry-run pre-commit fmt lint doc build python-venv python-build python-install \
        python-dev python-clean python-test

# Extract version from Cargo.toml
CURRENT_VERSION := $(shell grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
UV := uv

help:
	@echo "Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  pre-commit       - Run all checks before committing (format, lint, tests, docs)"
	@echo "  check            - Run cargo check"
	@echo "  build            - Build the project"
	@echo "  test             - Run tests"
	@echo "  fmt              - Format code with rustfmt"
	@echo "  lint             - Run clippy linter"
	@echo "  doc              - Build documentation"
	@echo "  doc-test         - Run documentation tests"
	@echo "  clean            - Clean build artifacts"
	@echo ""
	@echo "Python Bindings (using UV):"
	@echo "  python-venv      - Create Python virtual environment with UV"
	@echo "  python-build     - Build Python extension with maturin"
	@echo "  python-install   - Build and install into venv"
	@echo "  python-dev       - Build in development mode (editable install)"
	@echo "  python-clean     - Clean Python build artifacts"
	@echo "  python-test      - Test Python bindings"
	@echo ""
	@echo "Release Management:"
	@echo "  release-patch    - Bump patch version and release (e.g., 1.0.4 -> 1.0.5)"
	@echo "  release-minor    - Bump minor version and release (e.g., 1.0.4 -> 1.1.0)"
	@echo "  release-major    - Bump major version and release (e.g., 1.0.4 -> 2.0.0)"
	@echo "  release          - Custom version release (requires VERSION=x.y.z)"
	@echo ""
	@echo "Publishing:"
	@echo "  publish          - Publish to crates.io (requires VERSION=x.y.z)"
	@echo "  dry-run          - Dry run publish"
	@echo "  tag              - Create and push git tag (requires VERSION=x.y.z)"
	@echo ""
	@echo "Current version: $(CURRENT_VERSION)"

# Development commands
check:
	@echo "Running cargo check..."
	@cargo check --workspace --all-features

build:
	@echo "Building project..."
	@cargo build --workspace --all-features

test:
	@echo "Running tests..."
	@cargo test --workspace --all-features --lib --bins

fmt:
	@echo "Formatting code..."
	@cargo fmt --all

lint:
	@echo "Running clippy..."
	@./lint.sh --workspace --lib --bins --all-features -- -D warnings

doc:
	@echo "Building documentation..."
	@cargo doc --workspace --all-features --no-deps

doc-test:
	@echo "Running documentation tests..."
	@cargo test --workspace --doc --all-features

clean:
	@echo "Cleaning build artifacts..."
	@cargo clean

pre-commit: fmt lint check test doc-test
	@echo ""
	@echo "✓ All checks passed! Ready to commit."

# Publishing
dry-run:
	@echo "Dry run publishing ash-flare..."
	@cargo publish --dry-run

publish:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified"; \
		echo "Usage: make publish VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Publishing ash-flare $(VERSION) to crates.io..."
	@cargo publish

# Version management
tag:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified"; \
		echo "Usage: make tag VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Creating and pushing tag v$(VERSION)..."
	@git tag -a "v$(VERSION)" -m "Release version $(VERSION)"
	@git push origin "v$(VERSION)"
	@echo "✓ Tag v$(VERSION) created and pushed"

bump-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified"; \
		echo "Usage: make bump-version VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Bumping version to $(VERSION)..."
	@sed -i.bak 's/^version = ".*"/version = "$(VERSION)"/' Cargo.toml
	@rm -f Cargo.toml.bak
	@sed -i.bak 's/^version = ".*"/version = "$(VERSION)"/' pyproject.toml
	@rm -f pyproject.toml.bak
	@echo "✓ Updated version to $(VERSION)"

# Release process
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION not specified"; \
		echo "Usage: make release VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "=== Starting release $(VERSION) ==="
	@echo ""
	@echo "Step 1: Running pre-commit checks..."
	@$(MAKE) pre-commit
	@echo ""
	@echo "Step 2: Bumping version to $(VERSION)..."
	@$(MAKE) bump-version VERSION=$(VERSION)
	@echo ""
	@echo "Step 3: Committing version bump..."
	@git add Cargo.toml
	@git diff --cached --quiet || git commit -m "chore: bump version to $(VERSION)"
	@echo ""
	@echo "Step 4: Creating and pushing tag..."
	@$(MAKE) tag VERSION=$(VERSION)
	@echo "Call 'publish' to publish the new version to crates.io."

release-patch:
	@echo "=== Patch Release ==="
	@echo "Current version: $(CURRENT_VERSION)"
	@MAJOR=$$(echo $(CURRENT_VERSION) | cut -d. -f1); \
	MINOR=$$(echo $(CURRENT_VERSION) | cut -d. -f2); \
	PATCH=$$(echo $(CURRENT_VERSION) | cut -d. -f3); \
	NEW_PATCH=$$((PATCH + 1)); \
	NEW_VERSION="$$MAJOR.$$MINOR.$$NEW_PATCH"; \
	echo "New version: $$NEW_VERSION"; \
	echo ""; \
	$(MAKE) release VERSION=$$NEW_VERSION

release-minor:
	@echo "=== Minor Release ==="
	@echo "Current version: $(CURRENT_VERSION)"
	@MAJOR=$$(echo $(CURRENT_VERSION) | cut -d. -f1); \
	MINOR=$$(echo $(CURRENT_VERSION) | cut -d. -f2); \
	NEW_MINOR=$$((MINOR + 1)); \
	NEW_VERSION="$$MAJOR.$$NEW_MINOR.0"; \
	echo "New version: $$NEW_VERSION"; \
	echo ""; \
	$(MAKE) release VERSION=$$NEW_VERSION

release-major:
	@echo "=== Major Release ==="
	@echo "Current version: $(CURRENT_VERSION)"
	@MAJOR=$$(echo $(CURRENT_VERSION) | cut -d. -f1); \
	NEW_MAJOR=$$((MAJOR + 1)); \
	NEW_VERSION="$$NEW_MAJOR.0.0"; \
	echo "⚠️  WARNING: Major version bump ($$NEW_MAJOR.0.0)"; \
	echo "This indicates breaking changes!"; \
	echo "Press Enter to continue or Ctrl+C to cancel..."; \
	@read confirm; \
	MAJOR=$$(echo $(CURRENT_VERSION) | cut -d. -f1); \
	NEW_MAJOR=$$((MAJOR + 1)); \
	NEW_VERSION="$$NEW_MAJOR.0.0"; \
	$(MAKE) release VERSION=$$NEW_VERSION

# Python bindings targets
python-venv:
	@echo "Creating Python virtual environment with UV..."
	@if ! command -v uv > /dev/null 2>&1; then \
		echo "Error: UV is not installed. Please install UV first:"; \
		echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	fi
	@$(UV) venv $(VENV_DIR)
	@echo "✓ Virtual environment created at $(VENV_DIR)"
	@echo "Installing maturin..."
	@$(UV) pip install --python $(VENV_DIR) maturin
	@echo "✓ Maturin installed"

python-build: python-venv
	@echo "Building Python extension with maturin..."
	@$(VENV_DIR)/bin/maturin build --release --features python
	@echo "✓ Python extension built"

python-install: python-venv
	@echo "Building and installing Python extension..."
	@$(VENV_DIR)/bin/maturin develop --release --features python
	@echo "✓ Python extension installed into venv"
	@echo ""
	@echo "To use the Python bindings:"
	@echo "  source $(VENV_DIR)/bin/activate"
	@echo "  python -c 'import ash_flare; print(ash_flare)'"

python-dev: python-venv
	@echo "Building Python extension in development mode..."
	@$(VENV_DIR)/bin/maturin develop --features python
	@echo "✓ Python extension installed in development mode"
	@echo ""
	@echo "To use the Python bindings:"
	@echo "  source $(VENV_DIR)/bin/activate"
	@echo "  python -c 'import ash_flare; print(ash_flare)'"

python-clean:
	@echo "Cleaning Python build artifacts..."
	@rm -rf $(VENV_DIR)
	@rm -rf target/wheels
	@rm -rf *.so
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Python artifacts cleaned"

python-test: python-dev
	@echo "Testing Python bindings..."
	@$(PYTHON) -c "import ash_flare; print('✓ Module imports successfully')"
	@$(PYTHON) -c "from ash_flare import RestartPolicy; print('✓ RestartPolicy imported')"
	@$(PYTHON) -c "from ash_flare import RestartStrategy; print('✓ RestartStrategy imported')"
	@$(PYTHON) -c "from ash_flare import SupervisorSpec; print('✓ SupervisorSpec imported')"
	@echo "✓ All Python binding tests passed"
