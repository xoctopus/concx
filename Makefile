
# go package info
MODULE_PATH    := $(shell cat go.mod | grep ^module -m 1 | awk '{ print $$2; }' || '')
MODULE_NAME    := $(shell basename $(MODULE_PATH))
TEST_IGNORES   := "_gen.go|.pb.go|_mock.go|_genx_|main.go|testing.go|example/|testutil/|testdata/|hack/|vendor/"
FORMAT_IGNORES := ".git/,.xgo/,*.pb.go,*_genx_*,*_gen.go,*_mock.go,vendor/"

# git repository info
IS_GIT_REPO := $(shell git rev-parse --is-inside-work-tree >/dev/null 2>&1 && echo 1 || echo 0)
ifeq ($(IS_GIT_REPO),1)
export GIT_COMMIT_RAW := $(shell git rev-parse --short HEAD 2>/dev/null || echo "")
export GIT_COMMIT_AT  := $(shell git log -1 --format=%cd --date=format:%Y%m%d%H%M%S 2>/dev/null || echo "")
export GIT_TAG        := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
export GIT_BRANCH     := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
ifeq ($(shell git status --porcelain 2>/dev/null),)
export GIT_COMMIT := $(GIT_COMMIT_RAW)
else
export GIT_COMMIT := $(GIT_COMMIT_RAW)-dirty
endif
else
export GIT_COMMIT    := ""
export GIT_COMMIT_AT := ""
export GIT_TAG       := v0.0.0
export GIT_BRANCH    := ""
endif
export BUILD_AT := $(shell date "+%Y%m%d%H%M%S")
export MODULE_PATH

# global env variables
GOWORK ?= off
export GOWORK

# use vendor when the module is vendored
ifneq ($(wildcard vendor/modules.txt),)
export GOFLAGS := $(GOFLAGS) -mod=vendor
endif

# go build tools
GOTEST  := go
GOBUILD := go

# dependencies flags
DEP_DEVGEN            := $(shell type devgen > /dev/null 2>&1 && echo $$?)
DEP_GIT_CHGLOG        := $(shell type git-chglog > /dev/null 2>&1 && echo $$?)
DEP_GOIMPORTS_REVISER := $(shell type goimports-reviser > /dev/null 2>&1 && echo $$?)
DEP_GOLANGCI_LINT     := $(shell type golangci-lint > /dev/null 2>&1 && echo $$?)

show:
	@echo "module:"
	@echo "	path=$(MODULE_PATH)"
	@echo "	module=$(MODULE_NAME)"
	@echo "git:"
	@echo "	commit_id=$(GIT_COMMIT)"
	@echo "	commit_at=$(GIT_COMMIT_AT)"
	@echo "	tag=$(GIT_TAG)"
	@echo "	branch=$(GIT_BRANCH)"
	@echo "	build_at=$(BUILD_AT)"
	@echo "	name=$(MODULE_NAME)"
	@echo "tools:"
	@echo "	build=$(GOBUILD)"
	@echo "	test=$(GOTEST)"
	@echo "	devgen=$(shell which devgen) $(DEP_DEVGEN)"
	@echo "	git-chglog=$(shell which git-chglog) $(DEP_GIT_CHGLOG)"
	@echo "	goimports-reviser=$(shell which goimports-reviser) $(DEP_GOIMPORTS_REVISER)"
	@echo "	golangci-lint=$(shell which golangci-lint) $(DEP_GOLANGCI_LINT)"
	@echo "envs:"
	@echo "	GOWORK: $(GOWORK)"
	@echo "	GOFLAGS: $(GOFLAGS)"

dep:
	@echo "==> installing dependencies"
	@if [ "${DEP_DEVGEN}" != "0" ]; then \
		echo "	devgen for dev configuration generating"; \
		go install github.com/xoctopus/devx/cmd/devgen@main; \
		echo "	DONE."; \
	fi
	@if [ "${DEP_GIT_CHGLOG}" != "0" ]; then \
		echo "	git-chglog for generating changelog"; \
		go install github.com/git-chglog/git-chglog/cmd/git-chglog@latest; \
		echo "	DONE."; \
	fi
	@if [ "${DEP_GOIMPORTS_REVISER}" != "0" ]; then \
		echo "	goimports-reviser for code formating"; \
		go install github.com/incu6us/goimports-reviser/v3@latest; \
		echo "	DONE."; \
	fi
	@if [ "${DEP_GOLANGCI_LINT}" != "0" ]; then \
		echo "	golangci-lint for code linting"; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; \
		echo "	DONE."; \
	fi

upgrade-dep:
	@echo "==> upgrading dependencies"
	@echo "	devgen for dev configuration generating"
	@go install github.com/xoctopus/devx/cmd/devgen@main
	@echo "	DONE."
	@echo "	git-chglog for generating changelog"
	@go install github.com/git-chglog/git-chglog/cmd/git-chglog@latest
	@echo "	DONE."
	@echo "	goimports-reviser for code formating"
	@go install github.com/incu6us/goimports-reviser/v3@latest
	@echo "	DONE."
	@echo "	golangci-lint for code linting"
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@echo "	DONE."

tidy:
	@echo "==> go mod tidy"
	@go mod tidy
	@if [ -d vendor ]; then \
		echo "==> go mod vendor"; \
		go mod vendor; \
	fi

test: dep tidy
	@echo "==> run unit test"
	@$(GOTEST) test ./... -race -failfast -parallel 1 -gcflags="all=-N -l"

cover: dep tidy
	@echo "==> run unit test with coverage"
	@$(GOTEST) test ./... -bench=. -failfast -parallel 1 -gcflags="all=-N -l" -covermode=count -coverprofile=cover.out
	@grep -vE $(TEST_IGNORES) cover.out > cover2.out && mv cover2.out cover.out

view-cover: cover
	@echo "==> run unit test with coverage and view results"
	@$(GOBUILD) tool cover -html cover.out

ci-cover: lint cover


fmt: dep clean
	@echo "==> formating code"
	@goimports-reviser -rm-unused \
		-imports-order 'std,general,company,project' \
		-project-name ${MODULE_PATH} \
		-excludes $(FORMAT_IGNORES) ./...

fmt-check: fmt
	@echo "==> checking code format"
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "code is not properly formatted."; \
		echo "==> git status --porcelain"; \
		git status --porcelain; \
		echo "==> git diff"; \
		git diff; \
		exit 1; \
	fi

lint: dep
	@echo "==> linting"
	@echo ">>>golangci-lint"
	@golangci-lint run
	@go vet ./...
	@echo "done"

clean:
	@find . -name cover.out | xargs rm -rf
	@find . -name .xgo | xargs rm -rf
	@rm -rf build/*

changelog:
	@git chglog --next-tag HEAD -o CHANGELOG.md || true

pre-commit: dep fmt lint view-cover changelog
