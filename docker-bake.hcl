# Global variables
variable "CI_REGISTRY_IMAGE" {
    default = "registry.gitlab.syncad.com/hive/balance_tracker"
}
variable "TAG" {
  default = "latest"
}
variable "CI_COMMIT_SHORT_SHA" {
  default = ""
}
variable "CI_COMMIT_TAG" {
  default = ""
}
variable "CI_COMMIT_BRANCH" {
  default = "develop"
}
variable "CI_DEFAULT_BRANCH" {
  default = "develop"
}
variable "PSQL_CLIENT_VERSION" {
  default = "14"
}
variable "BUILD_TIME" {
  default = "${timestamp()}"
}
variable "GIT_COMMIT_SHA" {
  default = "[unknown]"
}
variable "GIT_CURRENT_BRANCH" {
  default = "[unknown]"
}
variable "GIT_LAST_LOG_MESSAGE" {
  default = "[unknown]"
}
variable "GIT_LAST_COMMITTER" {
  default = "[unknown]"
}
variable "GIT_LAST_COMMIT_DATE" {
  default = "[unknown]"
}

# Functions
function "notempty" {
  params = [variable]
  result = notequal("", variable)
}

function "registry-name" {
  params = [name, suffix]
  result = notempty(suffix) ? "${CI_REGISTRY_IMAGE}/${name}/${suffix}" : "${CI_REGISTRY_IMAGE}/${name}"
}

# Target groups
group "default" {
  targets = ["full"]
}

# Targets

## Locally tag image with "$TAG",
## which is "latest" by default
target "full" {
  dockerfile = "Dockerfile"
  target = "full"
  tags = [
    "${CI_REGISTRY_IMAGE}:${TAG}"
  ]
  args = {
    BUILD_TIME = "${BUILD_TIME}",
    GIT_COMMIT_SHA = "${GIT_COMMIT_SHA}",
    GIT_CURRENT_BRANCH = "${GIT_CURRENT_BRANCH}",
    GIT_LAST_LOG_MESSAGE = "${GIT_LAST_LOG_MESSAGE}",
    GIT_LAST_COMMITTER = "${GIT_LAST_COMMITTER}",
    GIT_LAST_COMMIT_DATE = "${GIT_LAST_COMMIT_DATE}",
  }
  platforms = [
    "linux/amd64"
  ]
  output = [
    "type=docker"
  ]
}

## Standardized tagging for CI builds:
## - Always: short commit SHA (for traceability)
## - develop branch: also 'latest'
## - Protected tags: also version tag
target "full-ci" {
  inherits = ["full"]
  contexts = {
    psql_client = "docker-image://${registry-name("psql-client", "")}:${PSQL_CLIENT_VERSION}"
  }
  cache-from = [
    "type=registry,ref=${registry-name("cache", "")}:${PSQL_CLIENT_VERSION}"
  ]
  cache-to = [
    "type=registry,mode=max,image-manifest=true,ref=${registry-name("cache", "")}:${PSQL_CLIENT_VERSION}"
  ]
  tags = [
    # Always tag with short commit SHA
    notempty(CI_COMMIT_SHORT_SHA) ? "${CI_REGISTRY_IMAGE}:${CI_COMMIT_SHORT_SHA}": "",
    # Tag with 'latest' on develop branch
    equal(CI_COMMIT_BRANCH, CI_DEFAULT_BRANCH) ? "${CI_REGISTRY_IMAGE}:latest": "",
    # Tag with version on protected tags
    notempty(CI_COMMIT_TAG) ? "${CI_REGISTRY_IMAGE}:${CI_COMMIT_TAG}": "",
  ]
  output = [
    "type=registry"
  ]
}
