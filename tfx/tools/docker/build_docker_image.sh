# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Convenience script to build TFX docker image.

set -ex

DOCKER_IMAGE_REPO=${DOCKER_IMAGE_REPO:-"tensorflow/tfx"}
DOCKER_IMAGE_TAG=${DOCKER_IMAGE_TAG:-"latest"}
DOCKER_FILE=${DOCKER_FILE:-"Dockerfile"}

# Base image to extend: This should be a deep learning image with a compatible
# TensorFlow version. See
# https://cloud.google.com/ai-platform/deep-learning-containers/docs/choosing-container
# for possible images to use here.
# Default value is set to latest `tf2-gpu` compatible image.
# TODO(156745950): Ideally, we should figure out which narrow version of
# TensorFlow current TFX code depends on here and use that instead.
if [[ -n "$BASE_IMAGE" ]]; then
  echo "Using override base image $BASE_IMAGE"
else
  BASE_IMAGE=$(gcloud container images list --repository="gcr.io/deeplearning-platform-release" | grep "tf2-gpu" | sort -n | tail -1)
  echo "Using latest tf2-gpu image $BASE_IMAGE as base"
fi


# Run docker build command.
docker build -t ${DOCKER_IMAGE_REPO}:${DOCKER_IMAGE_TAG} \
  -f tfx/tools/docker/${DOCKER_FILE} \
  --build-arg BASE_IMAGE=${BASE_IMAGE} \
  . "$@"
