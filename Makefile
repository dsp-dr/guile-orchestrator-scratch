# Guile Orchestrator Makefile
# Uses GNU Make (gmake on FreeBSD)

GUILE = guile3
GUILD = guild3
GUILE_FLAGS = --no-auto-compile -L src
GUILD_FLAGS = compile -L src

# Directories
SRC_DIR = src
TEST_DIR = tests
DOC_DIR = docs
BUILD_DIR = build

# Source files
SOURCES = $(shell find $(SRC_DIR) -name "*.scm" 2>/dev/null)
TESTS = $(shell find $(TEST_DIR) -name "*-test.scm" 2>/dev/null)
DOCS = $(shell find $(DOC_DIR) -name "*.org" 2>/dev/null)

# Compiled files
GOBJECTS = $(SOURCES:$(SRC_DIR)/%.scm=$(BUILD_DIR)/%.go)

# Default target
.PHONY: all
all: tangle compile

# Tangle org files to extract source code
.PHONY: tangle
tangle: $(DOCS)
	@echo "Tangling org files..."
	@for file in $(DOCS); do \
		emacs --batch \
			--eval "(require 'org)" \
			--eval "(setq org-confirm-babel-evaluate nil)" \
			--eval "(org-babel-tangle-file \"$$file\")" \
			2>/dev/null || true; \
	done

# Compile Scheme files
.PHONY: compile
compile: $(GOBJECTS)

$(BUILD_DIR)/%.go: $(SRC_DIR)/%.scm
	@mkdir -p $(dir $@)
	$(GUILD) $(GUILD_FLAGS) -o $@ $<

# Run tests
.PHONY: test
test: compile
	@echo "Running tests..."
	@$(GUILE) $(GUILE_FLAGS) $(TEST_DIR)/test-runner.scm

# Run specific test
.PHONY: test-one
test-one: compile
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: gmake test-one TEST=module-name"; \
		exit 1; \
	fi
	@$(GUILE) $(GUILE_FLAGS) -c "(use-modules (srfi srfi-64)) \
		(use-modules (tests $(TEST)-test)) \
		(exit (if (eqv? 0 (test-runner-fail-count (test-runner-current))) 0 1))"

# Run REPL with project modules
.PHONY: repl
repl:
	$(GUILE) $(GUILE_FLAGS)

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)
	find . -name "*.go" -delete
	find . -name "*~" -delete

# Clean everything including tangled files
.PHONY: distclean
distclean: clean
	@echo "Removing tangled source files..."
	@for file in $(DOCS); do \
		grep -h ":tangle" "$$file" 2>/dev/null | \
		sed -n 's/.*:tangle \([^ ]*\).*/\1/p' | \
		while read tangled; do \
			rm -f "$$tangled"; \
		done; \
	done

# Check code style
.PHONY: lint
lint:
	@echo "Checking code style..."
	@for file in $(SOURCES); do \
		echo "Checking $$file..."; \
		$(GUILE) -c "(use-modules (ice-9 pretty-print)) \
			(call-with-input-file \"$$file\" \
				(lambda (port) \
					(let loop () \
						(let ((expr (read port))) \
							(unless (eof-object? expr) \
								(pretty-print expr) \
								(loop))))))" > /dev/null || exit 1; \
	done

# Generate documentation
.PHONY: docs
docs: $(DOCS)
	@echo "Generating documentation..."
	@for file in $(DOCS); do \
		emacs --batch "$$file" \
			--eval "(require 'org)" \
			--eval "(org-html-export-to-html)" \
			2>/dev/null || true; \
	done

# Install (optional)
PREFIX ?= /usr/local
DESTDIR ?=

.PHONY: install
install: compile
	install -d $(DESTDIR)$(PREFIX)/share/guile/site/3.0
	cp -r $(SRC_DIR)/* $(DESTDIR)$(PREFIX)/share/guile/site/3.0/
	install -d $(DESTDIR)$(PREFIX)/lib/guile/3.0/site-ccache
	cp -r $(BUILD_DIR)/* $(DESTDIR)$(PREFIX)/lib/guile/3.0/site-ccache/

# Help target
.PHONY: help
help:
	@echo "Guile Orchestrator Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all       - Tangle and compile (default)"
	@echo "  tangle    - Extract source from org files"
	@echo "  compile   - Compile Scheme files"
	@echo "  test      - Run all tests"
	@echo "  test-one  - Run specific test (TEST=module-name)"
	@echo "  repl      - Start REPL with project modules"
	@echo "  clean     - Remove build artifacts"
	@echo "  distclean - Remove everything including tangled files"
	@echo "  lint      - Check code style"
	@echo "  docs      - Generate HTML documentation"
	@echo "  install   - Install to PREFIX (default: /usr/local)"
	@echo "  help      - Show this help"

.DEFAULT_GOAL := all