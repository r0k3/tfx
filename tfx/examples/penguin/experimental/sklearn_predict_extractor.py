# Copyright 2021 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Predict extractor for scikit-learn models."""

import copy
import os
import pickle
from typing import List, Optional, Sequence, Text

import apache_beam as beam
import numpy as np
import tensorflow as tf
import tensorflow_model_analysis as tfma
from tensorflow_model_analysis import constants
from tensorflow_model_analysis import model_util
from tensorflow_model_analysis import types
from tensorflow_model_analysis.extractors import extractor
from tfx_bsl.tfxio import tensor_adapter

PREDICT_EXTRACTOR_STAGE_NAME = 'SklearnPredict'


# pylint: disable=invalid-name
# pylint: disable=no-value-for-parameter
def SklearnPredictExtractor(
    eval_shared_model: tfma.MaybeMultipleEvalSharedModels,
    desired_batch_size: Optional[int] = None) -> extractor.Extractor:
  """Creates an extractor for performing predictions using a scikit-learn model.

  The extractor's PTransform loads and runs the serving pickle against
  every extract yielding a copy of the incoming extracts with an additional
  extract added for the predictions keyed by tfma.PREDICTIONS_KEY. The model
  inputs are searched for under tfma.INPUT_KEY(FEATURES_KEY?).

  Args:
    eval_shared_model: Shared model (single-model evaluation).
    desired_batch_size: Optional batch size for batching in Aggregate.

  Returns:
    Extractor for extracting predictions.
  """
  model_path = os.path.join(eval_shared_model.model_path, 'model.pkl')
  return extractor.Extractor(
      stage_name=PREDICT_EXTRACTOR_STAGE_NAME,
      ptransform=_ExtractPredictions(
          model_path=model_path,
          desired_batch_size=desired_batch_size))


@beam.typehints.with_input_types(beam.typehints.List[types.Extracts])
@beam.typehints.with_output_types(types.Extracts)
class _TFMAPredictionDoFn(model_util.BatchReducibleDoFnWithModels):
  """A DoFn that loads the models and predicts."""

  def __init__(self, model_path):
    super(_TFMAPredictionDoFn, self).__init__({})
    self.model = pickle.load(tf.io.gfile.GFile(model_path, 'rb'))

  def _batch_reducible_process(self, elements: List[types.Extracts]
                               ) -> Sequence[types.Extracts]:
    result = []
    for element in elements:
      element_copy = copy.copy(element)
      batch = element_copy[constants.FEATURES_KEY]
      features = []
      labels = []
      for features_dict in batch:
        features_row = [features_dict[key] for key in self.model.feature_keys]
        features.append(np.concatenate(features_row))
        labels.append(features_dict[self.model.label_key])

      preds = self.model.predict(features)
      element_copy[constants.PREDICTIONS_KEY] = preds
      element_copy[constants.LABELS_KEY] = np.concatenate(labels)
      result.append(element_copy)
    return result


@beam.ptransform_fn
@beam.typehints.with_input_types(types.Extracts)
@beam.typehints.with_output_types(types.Extracts)
def _ExtractPredictions(
    extracts: beam.pvalue.PCollection,
    model_path: Text,
    desired_batch_size: Optional[int]) -> beam.pvalue.PCollection:
  """A PTransform that adds predictions and possibly other tensors to extracts.

  Args:
    extracts: PCollection of extracts with inputs keyed by tfma.INPUTS_KEY.
    model_path: Path to stored model.
    desired_batch_size: Optional batch size.

  Returns:
    PCollection of Extracts updated with the predictions.
  """

  batch_args = {}

  if desired_batch_size is not None:
    batch_args = dict(
        min_batch_size=desired_batch_size, max_batch_size=desired_batch_size)

  return (
      extracts
      | 'Batch' >> beam.BatchElements(**batch_args)
      | 'Predict' >> beam.ParDo(_TFMAPredictionDoFn(model_path))
  )


# TFX Evaluator will call this function.
def custom_extractors(
    eval_shared_model: tfma.MaybeMultipleEvalSharedModels,
    eval_config: tfma.EvalConfig,
    tensor_adapter_config: tensor_adapter.TensorAdapterConfig,
) -> List[tfma.extractors.Extractor]:
  """Returns default extractors plus a custom prediction extractor."""
  predict_extractor = SklearnPredictExtractor(eval_shared_model)
  extractors = tfma.default_extractors(
      eval_shared_model=eval_shared_model,
      eval_config=eval_config,
      tensor_adapter_config=tensor_adapter_config,
      custom_predict_extractor=predict_extractor)

  # TODO(humichael): Remove once Transform is supported.
  extractors = [extract for extract in extractors
                if extract.stage_name != 'ExtractTransformedFeatures']
  return extractors


if __name__ == '__main__':
  pass
