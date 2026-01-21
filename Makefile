# Manticore Build System
REGISTRY ?= ghcr.io/cuppojoe
API_IMAGE ?= $(REGISTRY)/manticore-cpln-api
AGENT_IMAGE ?= $(REGISTRY)/manticore-cpln-agent
UI_IMAGE ?= $(REGISTRY)/manticore-cpln-ui

# Version can be overridden: make build-all VERSION=v1.0.0
VERSION ?= latest

# Platform for cross-compilation (default to amd64 for cloud deployments)
PLATFORM ?= linux/amd64

.PHONY: help build-api build-agent build-ui build-all push-api push-agent push-ui push-all clean

help: ## Show this help
	@echo "Manticore Build Targets"
	@echo ""
	@echo "Usage: make <target> [VERSION=v0.0.x] [PLATFORM=linux/amd64]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make build-all VERSION=v0.0.13"
	@echo "  make push-api VERSION=v0.0.3"
	@echo "  make release-agent VERSION=v0.1.0"
	@echo "  make release-all VERSION=v0.2.2 REGISTRY=ghcr.io/myuser"

# Build targets (using buildx for cross-platform support)
build-api: ## Build API (orchestrator) image
	@echo "Building $(API_IMAGE):$(VERSION) for $(PLATFORM)"
	docker buildx build --platform $(PLATFORM) -f ./build/api/Dockerfile -t $(API_IMAGE):$(VERSION) --load .
	@echo "Built $(API_IMAGE):$(VERSION)"

build-agent: ## Build agent image
	@echo "Building $(AGENT_IMAGE):$(VERSION) for $(PLATFORM)"
	docker buildx build --platform $(PLATFORM) -f ./build/agent/Dockerfile -t $(AGENT_IMAGE):$(VERSION) --load .
	@echo "Built $(AGENT_IMAGE):$(VERSION)"

build-ui: ## Build UI image
	@echo "Building $(UI_IMAGE):$(VERSION) for $(PLATFORM)"
	docker buildx build --platform $(PLATFORM) -f ./build/ui/Dockerfile -t $(UI_IMAGE):$(VERSION) --load ./ui
	@echo "Built $(UI_IMAGE):$(VERSION)"

build-all: build-api build-agent build-ui ## Build all images

# Push targets
push-api: ## Push API image to registry
	@echo "Pushing $(API_IMAGE):$(VERSION)"
	docker push $(API_IMAGE):$(VERSION)
	@echo "Pushed $(API_IMAGE):$(VERSION)"

push-agent: ## Push agent image to registry
	@echo "Pushing $(AGENT_IMAGE):$(VERSION)"
	docker push $(AGENT_IMAGE):$(VERSION)
	@echo "Pushed $(AGENT_IMAGE):$(VERSION)"

push-ui: ## Push UI image to registry
	@echo "Pushing $(UI_IMAGE):$(VERSION)"
	docker push $(UI_IMAGE):$(VERSION)
	@echo "Pushed $(UI_IMAGE):$(VERSION)"

push-all: push-api push-agent push-ui ## Push all images to registry

# Combined build and push
release-api: build-api push-api ## Build and push API image

release-agent: build-agent push-agent ## Build and push agent image

release-ui: build-ui push-ui ## Build and push UI image

release-all: build-all push-all ## Build and push all images

# Development helpers
dev-ui: ## Run UI dev server
	cd ui && pnpm dev

dev-ui-server: ## Run UI with Express proxy (for API testing)
	cd ui && ORCHESTRATOR_API_URL=http://localhost:8080 pnpm dev:server

dev-api: ## Run API in server mode
	MODE=server go run ./cmd/api

dev-agent: ## Run agent locally
	go run ./cmd/agent

# Clean
clean: ## Remove built artifacts
	cd ui && rm -rf dist node_modules/.cache
	@echo "Cleaned build artifacts"
