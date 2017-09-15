/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchToEstimateAndErrorBounds",
    value = "_FUNC_(sketch)",
    extended = "Returns a unique count estimate and error bounds from a given ArrayOfDoublesSketch."
    + " The result will be three double values:"
    + " estimate of the number of unique keys, lower bound and upper bound. The bounds are given"
    + " at 95.5% confidence.")
public class ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF extends UDF {

  /**
   * Get estimate, lower and upper bounds from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return estimate, lower bound and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        Memory.wrap(serializedSketch.getBytes()));
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(2), sketch.getUpperBound(2));
  }

}
