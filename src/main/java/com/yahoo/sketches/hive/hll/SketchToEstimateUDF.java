/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;

@Description(
    name = "SketchToEstimate",
    value = "_FUNC_(sketch)",
    extended = "Returns an estimate of unique count from a given HllSketch."
    + " The result is a double value.")
public class SketchToEstimateUDF extends UDF {

  /**
   * Get an estimate from a given HllSketch
   * @param serializedSketch HllSketch in a serialized binary form
   * @return estimate of unique count
   */
  public Double evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final HllSketch sketch = HllSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    return sketch.getEstimate();
  }

}
