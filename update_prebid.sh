#!/bin/bash
git fetch upstream
git merge upstream/master
go mod tidy
go mod vendor
