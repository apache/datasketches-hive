/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

@Description(name = "GetQuantile", value = "_FUNC_(sketch, fraction)",
extended = " Returns a quantile value from a given KllFloatsSketch."
+ " A single value for a given fraction is returned."
+ " The fraction represents a normalized rank, and must be from 0 to 1 inclusive."
+ " For example, a fraction of 0.5 corresponds to 50th percentile, which is"
+ " the median value of the distribution (the number separating the higher half"
+ " of the probability distribution from the lower half).")
public class GetQuantileFromSketchUDF extends UDF {

  /**
   * Returns a quantile value from a given sketch
   * @param serializedSketch serialized sketch
   * @param fraction value from 0 to 1 inclusive
   * @return quantile value
   */
  public Float evaluate(final BytesWritable serializedSketch, final double fraction) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    return sketch.getQuantile(fraction);
  }

}
