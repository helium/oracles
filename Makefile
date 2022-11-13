MAKEFLAGS := --no-print-directory

MINIO_ROOT_USER     := novaadmin
MINIO_ROOT_PASSWORD := $(MINIO_ROOT_USER)

AWS_ACCESS_KEY_ID     := $(MINIO_ROOT_USER)
AWS_SECRET_ACCESS_KEY := $(MINIO_ROOT_PASSWORD)

.PHONY: tests
tests:
	cargo check # Sanity check, to fail sooner.
	$(MAKE) tests_env_restart
	$(MAKE) tests_run

.PHONY: tests_run
tests_run:
	cd tests && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	cargo run --bin tests -- \
		--settings-ingest settings/ingestor.toml \
		--settings-verifier settings/mobile_verifier.toml \
		--settings-rewarder settings/mobile_rewarder.toml

.PHONY: tests_env_restart
tests_env_restart:
	$(MAKE) tests_env_stop
	$(MAKE) tests_env_start
	$(MAKE) tests_env_init

.PHONY: tests_env_start
tests_env_start:
	cd ./tests/local_infra/ && \
	MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	./infra create

.PHONY: tests_env_init
tests_env_init:
	cd ./tests/local_infra/ && ./infra init

.PHONY: tests_env_stop
tests_env_stop:
	cd ./tests/local_infra/ && ./infra destroy

.PHONY: clean
clean:
	rm -rf tests/data

.PHONY: all
all:
	$(MAKE) tests_env_stop
	$(MAKE) tests_env_start
	$(MAKE) tests_env_init
	$(MAKE) tests
