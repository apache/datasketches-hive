/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;

@Description(name = "GetQuantile", value = "_FUNC_(sketch, fraction)",
extended = " Returns a quantile value from a given DoublesSketch sketch."
+ " A single value for a given fraction is returned."
+ " The fraction represents a normalized rank, and must be from 0 to 1 inclusive."
+ " For example, a fraction of 0.5 corresponds to 50th percentile, which is"
+ " the median value of the distribution (the number separating the higher half"
+ " of the probability distribution from the lower half).")
public class GetQuantileFromDoublesSketchUDF extends UDF {

  public Double evaluate(final BytesWritable serializedSketch, final double fraction) {
    if (serializedSketch == null) return null;
    final DoublesSketch sketch = DoublesSketch.heapify(new NativeMemory(serializedSketch.getBytes()));
    return sketch.getQuantile(fraction);
  }

}
