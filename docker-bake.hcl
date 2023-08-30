# Global variables
variable "CI_REGISTRY_IMAGE" {
    default = "registry.gitlab.syncad.com/hive/balance_tracker"
}
variable "CI_COMMIT_SHA" {}
variable "CI_COMMIT_TAG" {}
variable "TAG" {
  default = "latest"
}
variable "TAG_CI" {
  default = "docker-24.0.1-3"
}
variable "BASE_TAG" {
  default = "ubuntu-22.04-1"
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
  targets = ["backend-block-processing", "backend-setup"]
}

group "base" {
  targets = ["backend-block-processing-base", "backend-setup-base"]
}

group "ci" {
  targets = ["backend-block-processing-ci", "backend-setup-ci"]
}

# Targets
target "backend-block-processing-base" {
  dockerfile = "Dockerfile.backend-block-processing"
  target = "base"
  tags = [
    "${registry-name("backend-block-processing", "base")}:${BASE_TAG}"
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend-block-processing" {
  dockerfile = "Dockerfile.backend-block-processing"
  tags = [
    "${registry-name("backend-block-processing", "")}:${TAG}",
    notempty(CI_COMMIT_SHA) ? "${registry-name("backend-block-processing", "")}:${CI_COMMIT_SHA}": "",
    notempty(CI_COMMIT_TAG) ? "${registry-name("backend-block-processing", "")}:${CI_COMMIT_TAG}": ""
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend-block-processing-ci" {
  inherits = ["backend-block-processing"]
  contexts = {
    base = "docker-image://${registry-name("backend-block-processing", "base")}:${BASE_TAG}"
  }
  cache-from = [
    "type=registry,ref=${registry-name("backend-block-processing", "cache")}:${TAG}"
  ]
  cache-to = [
    "type=registry,mode=max,ref=${registry-name("backend-block-processing", "cache")}:${TAG}"
  ]
  output = [
    "type=registry"
  ]
}

target "backend-setup-base" {
  dockerfile = "Dockerfile.backend-setup"
  target = "base"
  tags = [
    "${registry-name("backend-setup", "base")}:${BASE_TAG}"
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend-setup" {
  dockerfile = "Dockerfile.backend-setup"
  tags = [
    "${registry-name("backend-setup", "")}:${TAG}",
    notempty(CI_COMMIT_SHA) ? "${registry-name("backend-setup", "")}:${CI_COMMIT_SHA}": "",
    notempty(CI_COMMIT_TAG) ? "${registry-name("backend-setup", "")}:${CI_COMMIT_TAG}": ""
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend-setup-ci" {
  inherits = ["backend-setup"]
  contexts = {
    base = "docker-image://${registry-name("backend-setup", "base")}:${BASE_TAG}"
  }
  cache-from = [
    "type=registry,ref=${registry-name("backend-setup", "cache")}:${TAG}"
  ]
  cache-to = [
    "type=registry,mode=max,ref=${registry-name("backend-setup", "cache")}:${TAG}"
  ]
  output = [
    "type=registry"
  ]
}

target "ci-runner" {
  dockerfile = "Dockerfile"
  context = "docker/ci"
  tags = [
    "${registry-name("ci-runner", "")}:${TAG_CI}"
  ]
}

target "ci-runner-ci" {
  inherits = ["ci-runner"]
  cache-from = [
    "type=registry,ref=${registry-name("ci-runner", "cache")}:${TAG_CI}"
  ]
  cache-to = [
    "type=registry,mode=max,ref=${registry-name("ci-runner", "cache")}:${TAG_CI}"
  ]
  tags = [
    "${registry-name("ci-runner", "")}:${TAG_CI}"
  ]
}