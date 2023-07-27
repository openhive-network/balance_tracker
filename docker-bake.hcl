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
  default = "docker-24.0.1-2"
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
  targets = ["backend"]
}

group "ci" {
  targets = ["backend-ci"]
}

# Targets
target "backend-base" {
  dockerfile = "Dockerfile.backend"
  target = "base"
  tags = [
    "${registry-name("backend", "base")}:${BASE_TAG}"
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend" {
  dockerfile = "Dockerfile.backend"
  tags = [
    "${registry-name("backend", "")}:${TAG}",
    notempty(CI_COMMIT_SHA) ? "${registry-name("backend", "")}:${CI_COMMIT_SHA}": "",
    notempty(CI_COMMIT_TAG) ? "${registry-name("backend", "")}:${CI_COMMIT_TAG}": ""
  ]
  platforms = [
    "linux/amd64"
  ]
}

target "backend-ci" {
  inherits = ["backend"]
  contexts = {
    base = "docker-image://${registry-name("backend", "base")}:${BASE_TAG}"
  }
  cache-from = [
    "type=registry,ref=${registry-name("backend", "cache")}:${TAG}"
  ]
  cache-to = [
    "type=registry,mode=max,ref=${registry-name("backend", "cache")}:${TAG}"
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