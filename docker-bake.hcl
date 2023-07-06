# Global variables
variable "CI_REGISTRY_IMAGE" {
    default = "registry.gitlab.syncad.com/hive/balance_tracker"
}
variable "CI_COMMIT_SHA" {}
variable "CI_COMMIT_TAG" {}
variable "TAG" {
  default = "latest"
}
variable "BASE-TAG" {
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
    "${registry-name("backend", "base")}:${BASE-TAG}"
  ]
}

target "backend" {
  dockerfile = "Dockerfile.backend"
  tags = [
    "${registry-name("backend", "")}:${TAG}",
    notempty(CI_COMMIT_SHA) ? "${registry-name("backend", "")}:${CI_COMMIT_SHA}": "",
    notempty(CI_COMMIT_TAG) ? "${registry-name("backend", "")}:${CI_COMMIT_TAG}": ""
  ]
}

target "backend-ci" {
  inherits = ["backend"]
  contexts = {
    base = "${registry-name("backend", "base")}:${BASE-TAG}"
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