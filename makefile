
.ONESHELL:
.SHELL := /usr/bin/bash
.PHONY: apply destroy destroy-target plan-destroy plan plan-target prep
VARS="environments/$(ENV)-$(REGION).tfvars"
WORKSPACE="$(ENV)-$(REGION)"
BOLD=$(shell tput bold)
RED=$(shell tput setaf 1)
GREEN=$(shell tput setaf 2)
YELLOW=$(shell tput setaf 3)
RESET=$(shell tput sgr0)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

set-env:
	@if [ -z $(ENV) ]; then \
		echo "$(BOLD)$(RED)ENV was not set$(RESET)"; \
		ERROR=1; \
	 fi
	@if [ -z $(REGION) ]; then \
		echo "$(BOLD)$(RED)REGION was not set$(RESET)"; \
		ERROR=1; \
	 fi
	@if [ -z $(AWS_PROFILE) ]; then \
		echo "$(BOLD)$(RED)AWS_PROFILE was not set.$(RESET)"; \
		ERROR=1; \
	 fi
	@if [ ! -z $${ERROR} ] && [ $${ERROR} -eq 1 ]; then \
		echo "$(BOLD)Example usage: \`AWS_PROFILE=whatever ENV=demo REGION=us-east-2 make plan\`$(RESET)"; \
		exit 1; \
	 fi
	@if [ ! -f "$(VARS)" ]; then \
		echo "$(BOLD)$(RED)Could not find variables file: $(VARS)$(RESET)"; \
		exit 1; \
	 fi

prep: set-env ## Prepare a new workspace (environment) if needed, configure the tfstate backend, update any modules, and switch to the workspace
	@echo "$(BOLD)Configuring the terraform backend$(RESET)"
	@terraform init \
		-input=false \
		-force-copy \
		-lock=true \
		-upgrade \
		-verify-plugins=true \
		-backend=true 
	@echo "$(BOLD)Switching to workspace $(WORKSPACE)$(RESET)"
	@terraform workspace select $(WORKSPACE) || terraform workspace new $(WORKSPACE)

plan: prep ## Show what terraform thinks it will do
	@terraform plan \
		-lock=true \
		-input=false \
		-refresh=true \
		-var-file="$(VARS)"

plan-target: prep ## Shows what a plan looks like for applying a specific resource
	@echo "$(YELLOW)$(BOLD)[INFO]   $(RESET)"; echo "Example to type for the following question: module.rds.aws_route53_record.rds-master"
	@read -p "PLAN target: " DATA && \
		terraform plan \
			-lock=true \
			-input=true \
			-refresh=true \
			-var-file="$(VARS)" \
			-target=$$DATA

plan-destroy: prep ## Creates a destruction plan.
	@terraform plan \
		-input=false \
		-refresh=true \
		-destroy \
		-var-file="$(VARS)"

apply: prep ## Have terraform do the things. This will cost money.
	@terraform apply \
		-lock=true \
		-input=false \
		-refresh=true \
		-var-file="$(VARS)"

destroy: prep ## Destroy the things
	@terraform destroy \
		-lock=true \
		-input=false \
		-refresh=true \
		-var-file="$(VARS)"

destroy-target: prep ## Destroy a specific resource. Caution though, this destroys chained resources.
	@echo "$(YELLOW)$(BOLD)[INFO] Specifically destroy a piece of Terraform data.$(RESET)"; echo "Example to type for the following question: module.rds.aws_route53_record.rds-master"
	@read -p "Destroy target: " DATA && \
		terraform destroy \
		-lock=true \
		-input=false \
		-refresh=true \
		-var-file=$(VARS) \
		-target=$$DATA

