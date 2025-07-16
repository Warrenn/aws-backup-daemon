#!/bin/bash

git add .
git commit -m "$0"
git tag "$0"
git push origin
git push origin tag "$0"