IMAGE=gitlab-k8s-runner
CURRENT_DIR=$(shell pwd)
IMAGE_TAG=0.0.4
ACCOUNT=omhq
DOCKER_FILE=Dockerfile

.PHONY: clean
clean:
	docker compose rm -vf

.PHONY: run
run:
	docker compose up $(IMAGE); docker compose down --remove-orphans

.PHONY: shell
shell:
	docker compose run -it --entrypoint=/bin/bash $(IMAGE); docker compose down --remove-orphans

.PHONY: all
all: build tag push

.PHONY: build
build:
	DOCKER_BUILDKIT=1 \
	docker build \
	--platform=linux/amd64 \
	-t $(IMAGE):$(IMAGE_TAG) \
	-f $(CURRENT_DIR)/$(DOCKER_FILE) .

.PHONY: tag
tag:
	docker tag $(IMAGE):$(IMAGE_TAG) $(ACCOUNT)/$(IMAGE):$(IMAGE_TAG)

.PHONY: push
push:
	docker push $(ACCOUNT)/$(IMAGE):$(IMAGE_TAG)
