SHELL := /bin/bash
BASE_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
DATASETS_PATH=~/Desktop/DMSpark/datasets

run-pyspark:
	@docker run --rm -ti -v $(DATASETS_PATH):/data -p 4040:4040 stebourbi/sio:pyspark

open-spark-ui:
	@open http://localhost:4040

prepare-dev-env:
	@source $(BASE_DIR)/create-virtual-dev-env.sh

