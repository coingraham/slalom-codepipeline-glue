# Tapestry Terraform Template Repo

This repo is a template that contains a structured layout and sample TF code for Terraform repos. 

Each Repository will consist of a single Terraform Root or Module.

## Code Layout

For a typical Terraform root the file and folder layout is below along with explanations of each file.
```
.
├── README.md
├── .gitignore
├── atlantis.yaml
├── environments
│   ├── nonprod-us-east-1.tfvars
│   └── prod-us-east-1.tfvars
├── main.tf
├── outputs.tf
├── variables.tf
└── tf-cloud-templaterepo.code-workspace
```

### README.md

Every repository needs a README file. Many git repositories (especially on Github) have adopted Markdown as a de facto standard format for README files. A good README file will include the following information:

Overview: A brief description of the infrastructure the repo builds. A high-level diagram is often an effective method of expressing this information. 
Pre-requisites: Installation instructions (or links thereto) for any software that must be installed before building or changing the code. For a majority of Tapestry Terraform Repos, Terraform will be the only requirement and that is for locally running a new deployment. 
It’s important that you do not neglect this basic documentation for two reasons (even if you think you’re the only one who will work on the codebase):

The obvious: Writing this critical information down in an easily viewable place makes it easier for all members of Tapestry to onboard onto your project and will prevent the need for a panicked knowledge transfer when projects change hands.
The not-so-obvious: The act of writing a description of the design clarifies your intent to yourself and will result in a cleaner design and a more coherent repository.

### .gitignore

Every repositories should have a .gitignore to tell Git not to commit certain file extensions. The one provided in this repo should suffice in most use cases for Terraform, but this can be modified if needed. 

### atlantis.yaml

This file tells Atlantis the file layout, projects, and workspaces to use in this repository.

In the below example, there are two projects, sampleapp-non-prod-us-east-1 with a workspace name of non-prod-us-east-1 and a prod version. The naming standard for projects is the "terraform root name"-"environment"-"region". The Environment will usually be the account the resources are being deployed into. 

```
version: 3
projects:
- name: sampleapp-nonprod-us-east-1
  dir: .
  workflow: default
  workspace: nonprod-us-east-1
  terraform_version: v0.12.0
- name: sampleapp-prod-us-east-1
  dir: .
  workflow: default
  workspace: prod-us-east-1
  terraform_version: v0.12.0
```

### environments folder

This folder contains the different tfvar files for each environment. Each tfvar file will contain the answers for the variables.tf. In this app there are two, environment, prod and non prod and both are in the us-east-1 region.

### main.tf

This contains the terraform configuration, provider configuration, data sources, module calls and resources to be built. Typically this isn't split out into multiple files, but it can be if it will help with the code organization.

### outputs.tf

This contains the outputs of the root. This can then be consumed by other Terraform roots or output to the console

### variables.tf

This contains the variables that the main.tf will use. Typically there will not be any answers to variables in this file, but defaults can be provided that will be overridden if a tfvar file contains answers to the variable. 

### tf-cloud-templaterepo.code-workspace

This is a code-workspace file VScode uses to map a VSCode workspace to a set of folders and files. Custom workspace settings can be saved as well if needed. This is optional and VSCode doesn't require this, but it is nice to have

## Usage

Atlantis will be used to plan and apply the code through GitHub Webhooks. The code can be run locally if you have CLI keys to AWS with permissions. Instructions on how to run Atlantis are located TBD.

### Running Terraform Locally

Requirements:
Terraform Version 0.12 and later
AWS CLI
CLI credentials to AWS with permissions to the state bucket, dynamo db table, and able to assume the terraform cross account role

This will initialize the root, download the provider and module
`terraform init`

Lists the workspaces
`terraform workspace list`

Selects the workspace to use
`terraform workspace select`

Creates a new workspace. Workspace names are always the "environment name"-"region"
`terraform workspace new "prod-us-east-1`

This will run Terraform plan and source a tfvars file for the answers
`terraform plan -var-file="environments/prod-us-east-1.tfvars"`

This will run Terraform Apply and source a tfvars file for the answers
`terraform apply -var-file="environments/prod-us-east-1.tfvars"`

This will run Terraform Destroy and source a tfvars file for the answers
`terraform destroy -var-file="environments/prod-us-east-1.tfvars"`