/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.cpc.CpcSketch;

@Description(
    name = "GetEstimate",
    value = "_FUNC_(sketch)",
    extended = "Returns an estimate of unique count from a given CpcSketch."
    + " The result is a double value.")
public class GetEstimateUDF extends UDF {

  /**
   * Get an estimate from a given CpcSketch with explicit seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param seed update seed
   * @return estimate of unique count
   */
  public Double evaluate(final BytesWritable serializedSketch, final long seed) {
    if (serializedSketch == null) { return null; }
    final CpcSketch sketch = CpcSketch.heapify(Memory.wrap(serializedSketch.getBytes()), seed);
    return sketch.getEstimate();
  }

  /**
   * Get an estimate from a given CpcSketch using the default seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @return estimate of unique count
   */
  public Double evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_UPDATE_SEED);
  }

}
