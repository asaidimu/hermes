.PHONY: all test check fmt version bench bench-race

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

# @note #improvement-bench-ci-001 issue P3 status=proposed priority=P3 tags=#improvement,#ci,#testing : No CI gate for benchmark race-safety or regressions
#
# The critical-section benchmark suite (pkg/{pipeline,actionlog,registry,
# scheduler,events,store}/critical_bench_test.go and the per-fix benchmark
# files) lives in the repo but is not run by CI. `make bench` runs the
# suite under plain `go test -bench`; `make bench-race` runs a
# representative subset under the race detector with bounded iteration
# counts so it fits in a CI minute budget. Both targets skip the
# AnsiStore SQLite-backed benchmarks by default (they are O(milliseconds)
# per op and need `-benchtime=Nx`); see pkg/actionlog/critical_bench_test.go
# for the explicit invocation.
bench:
        go test -run='^$$' -bench=. -benchmem -benchtime=1s ./pkg/pipeline/ ./pkg/actionlog/ ./pkg/registry/ ./pkg/scheduler/ ./pkg/events/ ./pkg/store/ ./pkg/replay/

# bench-race runs a representative subset of benchmarks under the race
# detector with bounded iterations. The race detector's overhead makes
# full -benchtime=1s runs impractical for CI; -benchtime=20x keeps the
# total runtime under ~2 minutes while still exercising every concurrent
# path the benchmarks target.
bench-race:
        go test -race -run='^$$' -bench=. -benchmem -benchtime=20x \
                ./pkg/pipeline/ \
                ./pkg/registry/ \
                ./pkg/scheduler/ \
                ./pkg/events/ \
                ./pkg/store/ \
                ./pkg/replay/ \
                ./pkg/actionlog/

version:
        @echo $(VERSION)

release:
        @echo $(RELEASE)
