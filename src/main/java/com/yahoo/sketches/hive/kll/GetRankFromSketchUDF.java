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

@Description(name = "GetRank", value = "_FUNC_(sketch, value)",
extended = " Returns a normalized rank of a given value from a given KllFloatsSketch."
+ " The returned rank is an approximation to the fraction of values of the distribution"
+ " that are less than the given value (mass of the distribution below the given value).")
public class GetRankFromSketchUDF extends UDF {

  /**
   * Returns a normalized rank of a given value from a given sketch
   * @param serializedSketch serialized sketch
   * @param value the given value
   * @return rank
   */
  public Double evaluate(final BytesWritable serializedSketch, final float value) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    return sketch.getRank(value);
  }

}
