# Copyright 2021 Google LLC. All Rights Reserved.
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
"""TODO(muyangy): DO NOT SUBMIT without one-line documentation for execution_utils.

TODO(muyangy): DO NOT SUBMIT without a detailed description of execution_utils.
"""

from tfx.orchestration.portable import constants
from tfx.proto.orchestration import execution_result_pb2

from google.protobuf import json_format
from ml_metadata.proto import metadata_store_pb2


def set_execution_result(execution_result: execution_result_pb2.ExecutionResult,
                         execution: metadata_store_pb2.Execution):
  execution.custom_properties[constants.EXECUTION_RESULT].string_value = (
      json_format.MessageToJson(execution_result))


def get_execution_result(
    execution: metadata_store_pb2.Execution
) -> execution_result_pb2.ExecutionResult:
  if constants.EXECUTION_RESULT not in execution.custom_properties:
    raise ValueError(f'execution {execution} does not has '
                     f'{constants.EXECUTION_RESULT} as a custom property')

  return json_format.Parse(
      execution.custom_properties[constants.EXECUTION_RESULT].string_value,
      execution_result_pb2.ExecutionResult())
