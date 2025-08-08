# Simple Makefile for Parquet Analyzer testing

.PHONY: test install clean help

help:
	@echo "Available commands:"
	@echo "  make test      - Run all tests"
	@echo "  make install   - Install dependencies"
	@echo "  make clean     - Clean up"

test:
	uv run pytest tests/ -v

install:
	uv sync --extra dev

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf .pytest_cache/
