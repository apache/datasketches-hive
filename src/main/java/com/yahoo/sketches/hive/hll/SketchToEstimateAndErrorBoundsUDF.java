/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;

@Description(
    name = "SketchToEstimateAndErrorBounds",
    value = "_FUNC_(sketch, kappa)",
    extended = "Returns an estimate of distinct count and error bounds from a given HllSketch."
    + " Optional kappa is a number of standard deviations from the mean: 1, 2 or 3 (default 2)."
    + " The result is three double values: estimate, lower bound and upper bound.")
@SuppressWarnings("javadoc")
public class SketchToEstimateAndErrorBoundsUDF extends UDF {

  private static final int DEFAULT_KAPPA = 2;

  /**
   * Get an estimate and error bounds from a given HllSketch with default kappa
   * @param serializedSketch HllSketch in a serialized binary form
   * @return estimate and bounds
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_KAPPA);
  }

  /**
   * Get an estimate and error bounds from a given HllSketch with explicit kappa
   * @param serializedSketch HllSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @return estimate and bounds
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa) {
    if (serializedSketch == null) { return null; }
    final HllSketch sketch = HllSketch.wrap(Memory.wrap(serializedSketch.getBytes()));
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(kappa), sketch.getUpperBound(kappa));
  }

}
