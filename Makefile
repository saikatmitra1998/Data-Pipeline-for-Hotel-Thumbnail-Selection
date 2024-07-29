# Variables
IMAGE_NAME = main-image-selection
DATA_DIR = $(shell pwd)/data
OUTPUT_DIR = $(shell pwd)/output

# Default target to build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Target to run the Docker container
run: build
	docker run -v $(DATA_DIR):/app/data -v $(OUTPUT_DIR):/app/output $(IMAGE_NAME)

# Clean target to remove Docker image
clean:
	docker rmi $(IMAGE_NAME)

# Target to remove the output files
clean-output:
	rm -rf output/*

# Help target to display available commands
help:
	@echo "Available targets:"
	@echo "  build        - Build the Docker image"
	@echo "  run          - Build and run the Docker container"
	@echo "  clean        - Remove the Docker image"
	@echo "  clean-output - Remove all files in the output directory"
	@echo "  help         - Display this help message"

.PHONY: build run clean clean-output help