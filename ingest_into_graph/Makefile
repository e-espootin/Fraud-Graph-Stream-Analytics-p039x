# Variables
ENV_NAME = .venv
PYTHON = $(ENV_NAME)/bin/python
PIP = $(ENV_NAME)/bin/pip
REQ_FILE = requirements.txt
interval_sec=3

# Targets
.PHONY: all env install clean run

# Default target
all: env install

# Create virtual environment
env:
	python3 -m venv $(ENV_NAME)

# Install dependencies
install: env
	$(PIP) install -r $(REQ_FILE)

# Clean the environment
clean:
	rm -rf $(ENV_NAME)

# Run the project
run:
	$(PYTHON) main.py --interval_sec $(interval_sec)



