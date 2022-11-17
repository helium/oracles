MAKEFLAGS := --no-print-directory

MINIO_ROOT_USER     := novaadmin
MINIO_ROOT_PASSWORD := $(MINIO_ROOT_USER)

AWS_ACCESS_KEY_ID     := $(MINIO_ROOT_USER)
AWS_SECRET_ACCESS_KEY := $(MINIO_ROOT_PASSWORD)

.PHONY: tests_integration
tests_integration:
	cargo check # Sanity check, to fail sooner.
	$(MAKE) tests_integration_env_restart
	$(MAKE) tests_integration_run

.PHONY: tests_unit
tests_unit:
	cargo test

.PHONY: tests_all
tests_all:
	$(MAKE) tests_unit
	$(MAKE) tests_integration

.PHONY: tests_integration_run
tests_integration_run:
	cd tests && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	cargo run --bin tests -- \
		--settings-ingest settings/ingestor.toml \
		--settings-verifier settings/mobile_verifier.toml \
		--settings-rewarder settings/mobile_rewarder.toml

.PHONY: tests_integration_env_restart
tests_integration_env_restart:
	$(MAKE) tests_integration_env_stop
	$(MAKE) tests_integration_env_start
	$(MAKE) tests_integration_env_init

.PHONY: tests_integration_env_start
tests_integration_env_start:
	cd ./tests/local_infra/ && \
	MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	./infra create

.PHONY: tests_integration_env_init
tests_integration_env_init:
	cd ./tests/local_infra/ && ./infra init

.PHONY: tests_integration_env_stop
tests_integration_env_stop:
	cd ./tests/local_infra/ && ./infra destroy

.PHONY: clean
clean:
	rm -rf tests/data

.PHONY: all
all:
	$(MAKE) tests_integration_env_stop
	$(MAKE) tests_integration_env_start
	$(MAKE) tests_integration_env_init
	$(MAKE) tests_integration
