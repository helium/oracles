.PHONY: test
test:
	cd integration_tests && \
	cargo run --bin integration_tests -- \
		--settings-ingest settings/ingestor.toml \
		--settings-verifier settings/mobile_verifier.toml \
		--settings-rewarder settings/mobile_rewarder.toml

.PHONY: test_env_start
test_env_start:
	cd ./tests/local_infra/ && ./infra start

.PHONY: test_env_init
test_env_init:
	cd ./tests/local_infra/ && ./infra init

.PHONY: test_env_stop
test_env_stop:
	cd ./tests/local_infra/ && ./infra stop

.PHONY: clean
clean:
	rm -rf integration_tests/data

.PHONY: all
all:
	$(MAKE) test_env_stop
	$(MAKE) test_env_start
	$(MAKE) test_env_init
	$(MAKE) test
