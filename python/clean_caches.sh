#!/bin/bash
# Script to clear all Python and pytest cache files
# This fixes issues with stale bytecode causing import errors

echo "Clearing Python and pytest cache files..."

# Remove pytest cache
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null
echo "  ✓ Removed .pytest_cache directories"

# Remove Python bytecode cache
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -type f -name "*.pyc" -delete 2>/dev/null
find . -type f -name "*.pyo" -delete 2>/dev/null
echo "  ✓ Removed __pycache__ directories and .pyc/.pyo files"

echo "Cache cleared successfully!"

