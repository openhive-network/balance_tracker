# Global variables
variable "CI_REGISTRY_IMAGE" {
    default = "registry.gitlab.syncad.com/hive/balance_tracker"
}
variable "CI_COMMIT_SHORT_SHA" {
  default = ""
}
variable "CI_COMMIT_BRANCH" {
  default = "develop"
}
variable "CI_DEFAULT_BRANCH" {
  default = "develop"
}
variable "TAG_CI" {
  default = "docker-24.0.1-5"
}
variable "PSQL_CLIENT_VERSION" {
  default = "14"
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
  targets = ["psql-client"]
}

# Targets
target "psql-client" {
  dockerfile = "Dockerfile"
  target = "psql_client"
  tags = [
    "${registry-name("psql-client", "")}:${PSQL_CLIENT_VERSION}"
  ]
  platforms = [
    "linux/amd64"
  ]
}

## Locally tag image with "latest"
target "full" {
  inherits = ["psql-client"]
  target = "full"
  tags = [
    "${CI_REGISTRY_IMAGE}:latest"
  ]
}

## On develop, tag image with "latest" and commit hash,
## on any other branch tag image with just commit hash
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
    equal(CI_COMMIT_BRANCH, CI_DEFAULT_BRANCH) ? "${CI_REGISTRY_IMAGE}:latest": "",
    notempty(CI_COMMIT_SHORT_SHA) ? "${CI_REGISTRY_IMAGE}:${CI_COMMIT_SHORT_SHA}" : ""
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
    "type=registry,mode=max,image-manifest=true,ref=${registry-name("ci-runner", "cache")}:${TAG_CI}"
  ]
  tags = [
    "${registry-name("ci-runner", "")}:${TAG_CI}"
  ]
}