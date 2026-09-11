.PHONY: all test check fmt version

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
RELEASE ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev")

all: test

# @note #review-20260910-021 todo status=resolved priority=P2 tags=#review,#ci,#testing : CI ran `make test` without -race, vet, or gofmt gates
#
# Resolved: `make test` is now the full quality gate the project's own docs
# promise (README's "Testing & Quality Standard"): gofmt -s check, go vet,
# and go test -race. CI (.github/workflows/test.yaml) runs exactly this
# target, so the gates can no longer drift from local builds (see also the
# resolved #review-20260910-010, whose gofmt drift would have been caught
# here).
test:
	go clean -testcache && go vet ./... && go test -race ./...

# check is the fast subset CI can also use on its own: formatting + vet
# without the (slower) race test run.
check:
	@test -z "$$(gofmt -s -l .)" || (echo "gofmt -s needed on:" && gofmt -s -l . && exit 1)
	go vet ./...

# fmt normalizes the tree in place.
fmt:
	gofmt -s -w .

version:
	@echo $(VERSION)

release:
	@echo $(RELEASE)
