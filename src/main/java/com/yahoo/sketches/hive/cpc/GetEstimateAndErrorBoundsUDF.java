/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.cpc.CpcSketch;

@Description(
    name = "GetEstimateAndBounds",
    value = "_FUNC_(sketch, kappa, seed)",
    extended = "Returns an estimate and bounds of unique count from a given CpcSketch."
    + " The result is three double values: estimate, lower bound and upper bound."
    + " Optional kappa is a number of standard deviations from the mean: 1, 2 or 3 (default 2)."
    + " The seed is optional. It is needed if the sketch was created with a custom seed.")
public class GetEstimateAndErrorBoundsUDF extends UDF {

  private static final int DEFAULT_KAPPA = 2;

  /**
   * Get an estimate from a given CpcSketch using the default seed and kappa
   * @param serializedSketch CpcSketch in a serialized binary form
   * @return estimate of unique count
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_KAPPA, DEFAULT_UPDATE_SEED);
  }

  /**
   * Get an estimate and bounds from a given CpcSketch with default seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @return estimate, lower and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa) {
    return evaluate(serializedSketch, kappa, DEFAULT_UPDATE_SEED);
  }

  /**
   * Get an estimate and bounds from a given CpcSketch with explicit seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @param seed update seed
   * @return estimate, lower and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa, final long seed) {
    if (serializedSketch == null) { return null; }
    final CpcSketch sketch = CpcSketch.heapify(Memory.wrap(serializedSketch.getBytes()), seed);
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(kappa), sketch.getUpperBound(kappa));
  }

}
