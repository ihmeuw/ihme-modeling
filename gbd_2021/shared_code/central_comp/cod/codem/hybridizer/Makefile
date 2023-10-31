# Source files to format, lint, and type check.
LOCATIONS=hybridizer tests

PACKAGE_NAME = codem_hybridizer

CONDA_ENV_NAME ?= ${PACKAGE_NAME}

.DEFAULT_GOAL := list

.PHONY: list help install format lint typecheck test doc quick full

list help:
	@echo "Make targets:"
	@echo "[build-env] Make new conda env containing conda dependencies and poetry. Default name is ${PACKAGE_NAME}; uses CONDA_ENV_NAME variable if set"
	@echo "[install] Install pip dependencies, and install this package in editable mode"
	@echo "[format] Run the code formatter and the import sorter"
	@echo "[lint] Run the linters"
	@echo "[typecheck] Run the type checker"
	@echo "[test] Run the tests"
	@echo "[build-doc] Build the docs"
	@echo "[deploy-doc] Deploy docs: move them to docs directory (set via DOCS_ROOT_PATH and PACKAGE_VERSION) and symlink current docs"
	@echo "[build-package] Build the package (wheel and sdist)"
	@echo "[deploy-package] Deploy the package. Read PyPi username and password from environment variables"
	@echo
	@echo "[quick] Run a 'quick' build: format, lint, type check, tests, and docs"
	@echo "[full] Run a 'full' build: install, format, lint, type check, tests, and docs"

build-env:
	conda create -n $(CONDA_ENV_NAME) python=3.7 --yes

install:
	conda install --file conda_requirements.txt -y
	pip install .

format:
	black $(LOCATIONS)
	isort $(LOCATIONS)

lint:
	flake8 $(LOCATIONS)
	safety check

typecheck:
	# pytype --config=pytype.cfg $(LOCATIONS)
	@echo "skipping, no locations ready for typecheck"

test:
	pytest --cov --junitxml=test_report.xml
	coverage html -d htmlcov_test

build-doc:
	sphinx-apidoc -o docs -f hybridizer
	sphinx-build docs docs/_build

deploy-doc:
	mkdir -m 0775 -p "FILEPATH"
	cp -R docs/_build/* "FILEPATH"
	chmod -R 0775 "FILEPATH"
	cd "FILEPATH" current

build-package:
	python setup.py sdist

deploy-package:
	twine upload --repository ihme-artifactory ./dist/*

quick:
	$(MAKE) format lint typecheck test build-doc

full:
	$(MAKE) install format lint typecheck test build-doc
