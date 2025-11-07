#!/bin/bash
# Comprehensive test execution script

echo "========================================="
echo "Running Backend Test Suite"
echo "========================================="
echo ""

# Run tests with coverage
echo "📊 Running tests with coverage..."
python3 -m pytest tests/ \
    -v \
    --cov=. \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    --tb=short \
    2>&1 | tee test_results.log

echo ""
echo "========================================="
echo "Test Summary"
echo "========================================="

# Extract summary
grep "passed" test_results.log | tail -1

echo ""
echo "📈 Coverage report generated in htmlcov/index.html"
echo "📄 Full test results in test_results.log"
