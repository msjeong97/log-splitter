#!/bin/bash

set -xe

SCRIPT_PATH="$(dirname $0)"
PROJECT_PATH="${SCRIPT_PATH}/../"

cd $PROJECT_PATH
sbt clean assembly
