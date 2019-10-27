.PHONY: install develop docker

install:
	pip install .

develop:
	pip install -e .

docker:
	docker build -t verdict -f docker/Dockerfile .
