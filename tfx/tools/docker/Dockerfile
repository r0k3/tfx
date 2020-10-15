# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
ARG BASE_IMAGE

FROM gcr.io/tfx-oss-public/tfx_base:py37-20200729 as wheel-builder
# Change to following base image if docker hub is preferred.
# FROM tensorflow/tfx_base:latest
# docker build command should be run under root directory of github checkout.
ENV TFX_DIR=/tfx
ADD . ${TFX_DIR}/src
WORKDIR ${TFX_DIR}

ARG TFX_DEPENDENCY_SELECTOR
ENV TFX_DEPENDENCY_SELECTOR=${TFX_DEPENDENCY_SELECTOR}

# TODO(b/175089240): clean up conditional checks on whether ml-pipelines-sdk is
# built after TFX versions <= 0.25 are no longer eligible for cherry-picks.
RUN cd ${TFX_DIR}/src; \
    if [ -e "package_build" ]; then \
      bash -x package_build/initialize.sh; \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python package_build/ml-pipelines-sdk/setup.py bdist_wheel; \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python package_build/tfx/setup.py bdist_wheel; \
      MLSDK_WHEEL=$(find dist -name "ml_pipelines_sdk-*.whl"); \
      TFX_WHEEL=$(find dist -name "tfx-*.whl"); \
    else \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python setup.py bdist_wheel; \
      MLSDK_WHEEL=; \
      TFX_WHEEL=$(find dist -name "tfx-*.whl"); \
    fi; \
    if [ "${TFX_DEPENDENCY_SELECTOR}" = "NIGHTLY" ]; then \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python -m pip install \
          --extra-index-url https://pypi-nightly.tensorflow.org/simple \
          ${MLSDK_WHEEL} ${TFX_WHEEL}[docker-image] ; \
    else \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python -m pip install ${MLSDK_WHEEL} ${TFX_WHEEL}[docker-image] ; \
    fi;

# Build stage to extend DLVM image.

FROM ${BASE_IMAGE}

LABEL maintainer="tensorflow-extended-dev@googlegroups.com"

# TODO(b/171324626): remove libsnappy-dev to DLVM base image.
RUN apt-get update && apt-get install -y libsnappy-dev && apt-get clean all

# TODO(b/151392812): Remove `google-api-python-client` and `google-apitools`
#                    when patching is not needed any more.
RUN python -m pip install \
  "google-api-python-client==1.8.0" \
  "google-apitools==0.5.30"

COPY --from=wheel-builder /tfx/src/dist/*.whl /tfx/src/dist/ \
    /tfx/src/tfx/tools/docker/patches /tfx/src/tfx/tools/docker/patches
WORKDIR /tfx/src
RUN export MLSDK_WHEEL=$(find dist -name "ml_pipelines_sdk-*.whl") \
    TFX_WHEEL=$(find dist -name "tfx-*.whl")

RUN if [ "${TFX_DEPENDENCY_SELECTOR}" = "NIGHTLY" ]; then \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python -m pip install \
          --extra-index-url https://pypi-nightly.tensorflow.org/simple \
          ${MLSDK_WHEEL} ${TFX_WHEEL}[docker-image] ; \
    else \
      CFLAGS=$(/usr/bin/python-config --cflags) \
        python -m pip install ${MLSDK_WHEEL} ${TFX_WHEEL}[docker-image] ; \
    fi;

RUN echo "Installed python packages:\n" && python -m pip list

# Patch http.py in googleapiclient and base_api.py in apitools
# to use our own UserAgent.
# TODO(b/151392812): Remove this when other telemetries become available.
RUN patch `python -c 'import googleapiclient; print(googleapiclient.__path__[0])'`/http.py \
  /tfx/src/tfx/tools/docker/patches/http.patch && \
 patch `python -c 'import apitools; print(apitools.__path__[0])'`/base/py/base_api.py \
  /tfx/src/tfx/tools/docker/patches/base_api.patch

# Copy the boot loader program from latest released beam python sdk.
# TODO(b/156745950): Copy from image matching installed version of 'apache-beam'.
COPY --from=apache/beam_python3.7_sdk:latest /opt/apache/beam/boot /opt/apache/beam/boot
# Dataflow SDK worker image looks for pip at a different location by default
# without following pip environment variable.
RUN export pip=$(which pip);echo $pip;
ENTRYPOINT ["/opt/apache/beam/boot"]
