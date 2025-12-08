# Load .env if exists
-include .env

all: test report

test:
	@echo "Running tests..."
	@ginkgo -p -r -cover ./...

report:
	@echo "Generating coverage report..."
	@go tool cover -html=coverprofile.out -o coverage.html
	@echo "Coverage report generated at coverage.html"
