#!/bin/bash
set -xe

cd /build
./mvnw clean test
