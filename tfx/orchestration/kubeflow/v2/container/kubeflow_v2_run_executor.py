# Copyright 2020 Google LLC
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
"""Entrypoint for invoking TFX components in Kubeflow V2 runner."""

import argparse
import os
from typing import List

from absl import logging

from tfx.components.evaluator import executor as evaluator_executor
from tfx.dsl.components.base import base_executor
from tfx.dsl.io import fileio
from tfx.orchestration.kubeflow.v2.container import kubeflow_v2_entrypoint_utils
from tfx.orchestration.kubeflow.v2.proto import pipeline_pb2
from tfx.types import artifact_utils
from tfx.types.standard_component_specs import BLESSING_KEY
from tfx.utils import import_utils

from google.protobuf import json_format
from tensorflow.python.platform import app  # pylint: disable=g-direct-tensorflow-import


# TODO(b/166202742): Consolidate container entrypoint with Kubeflow runner.
# TODO(b/154046602): Consider put this function into tfx/orchestration, and
# unify the code paths to call into component executors.
def _run_executor(args: argparse.Namespace, beam_args: List[str]) -> None:
  """Selects a particular executor and run it based on name.

  Args:
    args:
      --executor_class_path: The import path of the executor class.
      --json_serialized_invocation_args: Full JSON-serialized parameters for
        this execution.
    beam_args: Optional parameter that maps to the optional_pipeline_args
      parameter in the pipeline, which provides additional configuration options
      for apache-beam and tensorflow.logging.
    For more about the beam arguments please refer to:
    https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
  """
  logging.set_verbosity(logging.INFO)

  # Rehydrate inputs/outputs/exec_properties from the serialized metadata.
  executor_input = pipeline_pb2.ExecutorInput()
  json_format.Parse(
      args.json_serialized_invocation_args,
      executor_input,
      ignore_unknown_fields=True)

  inputs_dict = executor_input.inputs.artifacts
  outputs_dict = executor_input.outputs.artifacts
  inputs_parameter = executor_input.inputs.parameters

  name_from_id = {}

  inputs = kubeflow_v2_entrypoint_utils.parse_raw_artifact_dict(
      inputs_dict, name_from_id)
  outputs = kubeflow_v2_entrypoint_utils.parse_raw_artifact_dict(
      outputs_dict, name_from_id)
  exec_properties = kubeflow_v2_entrypoint_utils.parse_execution_properties(
      inputs_parameter)
  logging.info('Executor %s do: inputs: %s, outputs: %s, exec_properties: %s',
               args.executor_class_path, inputs, outputs, exec_properties)
  executor_cls = import_utils.import_class_by_path(args.executor_class_path)
  executor_context = base_executor.BaseExecutor.Context(
      beam_pipeline_args=beam_args, unique_id='')
  executor = executor_cls(executor_context)
  logging.info('Starting executor')
  executor.Do(inputs, outputs, exec_properties)

  # TODO(b/169583143): Remove this workaround when TFX migrates to use str-typed
  # id/name to identify artifacts.
  # Convert ModelBlessing artifact to use managed MLMD resource name.
  if (issubclass(executor_cls, evaluator_executor.Executor) and
      BLESSING_KEY in outputs):
    # Parse the parent prefix for managed MLMD resource name.
    kubeflow_v2_entrypoint_utils.refactor_model_blessing(
        artifact_utils.get_single_instance(outputs[BLESSING_KEY]),
        name_from_id)

  # Log the output metadata to a file. So that it can be picked up by MP.
  metadata_uri = executor_input.outputs.output_file
  executor_output = pipeline_pb2.ExecutorOutput()
  for k, v in kubeflow_v2_entrypoint_utils.translate_executor_output(
      outputs, name_from_id).items():
    executor_output.artifacts[k].CopyFrom(v)

  fileio.makedirs(os.path.dirname(metadata_uri))
  fileio.open(metadata_uri,
              'wb').write(json_format.MessageToJson(executor_output))


def main(argv):
  """Parses the arguments for _run_executor() then invokes it.

  Args:
    argv: Unparsed arguments for run_executor.py. Known argument names include
      --executor_class_path: Python class of executor in format of
        <module>.<class>.
      --json_serialized_invocation_args: Full JSON-serialized parameters for
        this execution. The remaining part of the arguments will be parsed as
        the beam args used by each component executors. Some commonly used beam
        args are as follows:
        --runner: The beam pipeline runner environment. Can be DirectRunner (for
          running locally) or DataflowRunner (for running on GCP Dataflow
          service).
        --project: The GCP project ID. Neede when runner==DataflowRunner
        --direct_num_workers: Number of threads or subprocesses executing the
          work load.
        For more about the beam arguments please refer to:
        https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

  Returns:
    None

  Raises:
    None
  """

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--executor_class_path',
      type=str,
      required=True,
      help='Python class of executor in format of <module>.<class>.')
  parser.add_argument(
      '--json_serialized_invocation_args',
      type=str,
      required=True,
      help='JSON-serialized metadata for this execution.')

  args, beam_args = parser.parse_known_args(argv)
  _run_executor(args, beam_args)


if __name__ == '__main__':
  app.run(main=main)
