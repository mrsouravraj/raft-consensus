# Raft Consensus Algorithm - Educational Implementation
# Makefile for common development tasks

.PHONY: help install test test-verbose test-coverage clean demo demo-apps lint format type-check docs

# Default target
help:
	@echo "Raft Consensus Algorithm - Development Commands"
	@echo ""
	@echo "Available targets:"
	@echo "  help         Show this help message"
	@echo "  install      Install the package in development mode"
	@echo "  test         Run all tests"
	@echo "  test-verbose Run tests with verbose output"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  demo         Run basic Raft consensus demo"
	@echo "  demo-apps    Run real-world applications demo"
	@echo "  lint         Run code linting"
	@echo "  format       Format code with black"
	@echo "  type-check   Run type checking with mypy"
	@echo "  clean        Clean up build artifacts"
	@echo "  docs         Generate documentation"

# Install package in development mode
install:
	pip install -e .

# Install development dependencies
install-dev:
	pip install -e .[dev]

# Run tests
test:
	python -m unittest discover tests/ -v

# Run tests with pytest (if available)
test-pytest:
	python -m pytest tests/ -v

# Run tests with verbose output
test-verbose:
	python -m unittest discover tests/ -v

# Run tests with coverage (requires pytest-cov)
test-coverage:
	python -m pytest tests/ --cov=src/raft --cov-report=html --cov-report=term

# Run basic Raft demo
demo:
	python examples/basic_raft_demo.py

# Run real-world applications demo
demo-apps:
	python examples/real_world_applications_demo.py

# Run linting (requires flake8)
lint:
	python -m flake8 src/ tests/ examples/ --max-line-length=100 --ignore=E203,W503

# Format code (requires black)
format:
	python -m black src/ tests/ examples/ --line-length=100

# Run type checking (requires mypy)
type-check:
	python -m mypy src/raft --ignore-missing-imports

# Clean up build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Generate documentation (requires sphinx)
docs:
	@echo "Documentation generation not implemented yet"
	@echo "See README.md for comprehensive documentation"

# Check code quality (combines lint, format-check, type-check)
check: lint type-check
	@echo "Code quality checks completed"

# Quick development setup
dev-setup: install-dev
	@echo "Development environment setup completed"
	@echo "Run 'make test' to verify installation"

# CI/CD pipeline (for GitHub Actions or similar)
ci: test lint type-check
	@echo "CI pipeline completed successfully"
