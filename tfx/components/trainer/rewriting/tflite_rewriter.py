# Lint as: python2, python3
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Rewriter that invokes the TFLite converter."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

from typing import Optional, Sequence, Text

import six
import tensorflow as tf

from tfx.components.trainer.rewriting import rewriter
from tfx.dsl.io import fileio
from tfx.utils import io_utils

EXTRA_ASSETS_DIRECTORY = 'assets.extra'


def _create_tflite_converter(
    saved_model_path: Text,
    quantization_optimizations: Sequence[tf.lite.Optimize],
    quantization_supported_types: Sequence[tf.DType],
    # TODO(b/175699054): Enable once data API is adopted.
    input_data=None
) -> tf.lite.TFLiteConverter:
  """Creates a TFLite converter with proper quantization options.

  Currently,
  this supports DYNAMIC_RANGE, FULL_INTEGER and FLOAT16 quantizations.

  Args:
    saved_model_path: Path for the TF SavedModel.
    quantization_optimizations: Options for optimizations in quantization. If
      empty, no quantization will be applied(float32). Check
      https://www.tensorflow.org/lite/performance/post_training_quantization for
        details.
    quantization_supported_types: Options for optimizations in quantization.
      Check
      https://www.tensorflow.org/lite/performance/post_training_quantization for
        details.
    input_data: Data for full-integer quantization to be used as a
      representative dataset. None if quantization is not full-integer.

  Returns:
    A TFLite converter with the proper flags being set.

  Raises:
    NotImplementedError: Raises when full-integer quantization is called.
  """

  # TODO(b/175699054): Remove once data API is adopted.
  if input_data:
    raise NotImplementedError('Full-integer quantization is not supported yet.')

  converter = tf.lite.TFLiteConverter.from_saved_model(saved_model_path)

  converter.optimizations = quantization_optimizations
  converter.target_spec.supported_types = quantization_supported_types

  if input_data:
    def _representative_dataset():
      # TODO(b/175699054): Fill the logic once input_data is landed.
      pass
    converter.representative_dataset = _representative_dataset

  return converter


def _create_tflite_compatible_saved_model(src: Text, dst: Text):
  io_utils.copy_dir(src, dst)
  assets_path = os.path.join(dst, tf.saved_model.ASSETS_DIRECTORY)
  if fileio.exists(assets_path):
    fileio.rmtree(assets_path)
  assets_extra_path = os.path.join(dst, EXTRA_ASSETS_DIRECTORY)
  if fileio.exists(assets_extra_path):
    fileio.rmtree(assets_extra_path)


class TFLiteRewriter(rewriter.BaseRewriter):
  """Performs TFLite conversion."""

  def __init__(
      self,
      name: Text,
      filename: Text = 'tflite',
      copy_assets: bool = True,
      copy_assets_extra: bool = True,
      quantization_optimizations: Optional[Sequence[tf.lite.Optimize]] = None,
      quantization_supported_types: Optional[Sequence[tf.DType]] = None,
      quantization_enable_full_integer: bool = False):
    """Create an instance of the TFLiteRewriter.

    Args:
      name: The name to use when identifying the rewriter.
      filename: The name of the file to use for the tflite model.
      copy_assets: Boolean whether to copy the assets directory to the rewritten
        model directory.
      copy_assets_extra: Boolean whether to copy the assets.extra directory to
        the rewritten model directory.
      quantization_optimizations: Options for optimizations in quantization. If
        None, no quantization will be applied(float32). Check
        https://www.tensorflow.org/lite/performance/post_training_quantization
        for details.
      quantization_supported_types: Options for optimizations in quantization.
        Check
        https://www.tensorflow.org/lite/performance/post_training_quantization
        for details.
      quantization_enable_full_integer: True to quantizae with FULL_INTEGER
        option.
    """
    # TODO(b/152636072): Add support for representative_dataset.
    self._name = name
    self._filename = six.ensure_text(filename)
    self._copy_assets = copy_assets
    self._copy_assets_extra = copy_assets_extra

    if quantization_optimizations is None:
      quantization_optimizations = []
    if quantization_supported_types is None:
      quantization_supported_types = []
    self._quantization_optimizations = quantization_optimizations
    self._quantization_supported_types = quantization_supported_types
    self._input_data = None
    if quantization_enable_full_integer:
      # TODO(b/175699054): Enable once data API is landed.
      # This is to trigger FULL_INTEGER path inside _create_tflite_converter
      # for testing purpose. As a consequence the code will throw
      # NotImplementedError inside _create_tflite_converter.
      self._input_data = 1

  @property
  def name(self) -> Text:
    """The user-specified name of the rewriter."""
    return self._name

  def _pre_rewrite_validate(self, original_model: rewriter.ModelDescription):
    """Performs pre-rewrite checks to see if the model can be rewritten.

    Args:
      original_model: A `ModelDescription` object describing the model to be
        rewritten.

    Raises:
      ValueError: If the original model does not have the expected structure.
    """
    if original_model.model_type != rewriter.ModelType.SAVED_MODEL:
      raise ValueError('TFLiteRewriter can only convert SavedModels.')

  def _rewrite(self, original_model: rewriter.ModelDescription,
               rewritten_model: rewriter.ModelDescription):
    """Rewrites the provided model.

    Args:
      original_model: A `ModelDescription` specifying the original model to be
        rewritten.
      rewritten_model: A `ModelDescription` specifying the format and location
        of the rewritten model.

    Raises:
      ValueError: If the model could not be sucessfully rewritten.
    """
    if rewritten_model.model_type not in [
        rewriter.ModelType.TFLITE_MODEL, rewriter.ModelType.ANY_MODEL
    ]:
      raise ValueError('TFLiteConverter can only convert to the TFLite format.')

    # TODO(dzats): We create a temporary directory with a SavedModel that does
    # not contain an assets or assets.extra directory. Remove this when the
    # TFLite converter can convert models having these directories.
    tmp_model_dir = os.path.join(
        six.ensure_text(rewritten_model.path),
        'tmp-rewrite-' + str(int(time.time())))
    if fileio.exists(tmp_model_dir):
      raise ValueError('TFLiteConverter is unable to create a unique path '
                       'for the temp rewriting directory.')

    fileio.makedirs(tmp_model_dir)
    _create_tflite_compatible_saved_model(
        six.ensure_text(original_model.path), tmp_model_dir)

    converter = _create_tflite_converter(
        saved_model_path=tmp_model_dir,
        quantization_optimizations=self._quantization_optimizations,
        quantization_supported_types=self._quantization_supported_types,
        # TODO(b/175699054): Enable once data API is landed.
        input_data=self._input_data)
    tflite_model = converter.convert()

    output_path = os.path.join(
        six.ensure_text(rewritten_model.path), self._filename)
    with fileio.open(six.ensure_text(output_path), 'wb') as f:
      f.write(six.ensure_binary(tflite_model))
    fileio.rmtree(tmp_model_dir)

    copy_pairs = []
    if self._copy_assets:
      src = os.path.join(
          six.ensure_text(original_model.path), tf.saved_model.ASSETS_DIRECTORY)
      dst = os.path.join(
          six.ensure_text(rewritten_model.path),
          tf.saved_model.ASSETS_DIRECTORY)
      if fileio.isdir(src):
        fileio.mkdir(dst)
        copy_pairs.append((src, dst))
    if self._copy_assets_extra:
      src = os.path.join(
          six.ensure_text(original_model.path), EXTRA_ASSETS_DIRECTORY)
      dst = os.path.join(
          six.ensure_text(rewritten_model.path), EXTRA_ASSETS_DIRECTORY)
      if fileio.isdir(src):
        fileio.mkdir(dst)
        copy_pairs.append((src, dst))
    for src, dst in copy_pairs:
      io_utils.copy_dir(src, dst)

  def _post_rewrite_validate(self, rewritten_model: rewriter.ModelDescription):
    """Performs post-rewrite checks to see if the rewritten model is valid.

    Args:
      rewritten_model: A `ModelDescription` specifying the format and location
        of the rewritten model.

    Raises:
      ValueError: If the rewritten model is not valid.
    """
    # TODO(dzats): Implement post-rewrite validation.
    pass
