.PHONY: test_env_start
test_env_start:
	./tests/local_infra/infra start

.PHONY: test_env_stop
test_env_stop:
	./tests/local_infra/infra stop

.PHONY: clean
clean:
	rm -rf integration_tests/data
