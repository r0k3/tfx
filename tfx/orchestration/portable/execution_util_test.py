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
"""Tests for tfx.orchestration.portable.execution_util."""

import tensorflow as tf
from tfx.orchestration.portable import execution_utils
from tfx.proto.orchestration import execution_result_pb2

from google.protobuf import text_format
from ml_metadata.proto import metadata_store_pb2


class ExecutionUtilTest(tf.test.TestCase):

  def test_set_and_get_execution_result(self):
    execution = metadata_store_pb2.Execution()
    execution_result = text_format.Parse("""
        code: 1
        result_message: 'error message.'
      """, execution_result_pb2.ExecutionResult())
    execution_utils.set_execution_result(execution_result, execution)

    self.assertProtoEquals(
        """
          custom_properties {
            key: 'execution_result'
            value {
              string_value: '{\\n  "resultMessage": "error message.",\\n  "code": 1\\n}'
            }
          }
          """, execution)
    self.assertEqual(
        execution_utils.get_execution_result(execution), execution_result)

  def test_exectuion_result_not_found(self):
    execution = metadata_store_pb2.Execution()
    with self.assertRaisesRegex(
        ValueError,
        'execution .* does not has execution_result as a custom property'):
      _ = execution_utils.get_execution_result(execution)


if __name__ == '__main__':
  tf.test.main()
