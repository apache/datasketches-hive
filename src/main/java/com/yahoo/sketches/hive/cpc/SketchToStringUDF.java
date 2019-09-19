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
    name = "SketchToString",
    value = "_FUNC_(sketch, seed)",
    extended = "Returns a human-readable summary of a given CpcSketch.")
@SuppressWarnings("javadoc")
public class SketchToStringUDF extends UDF {

  /**
   * Get a summary of a given CpcSketch with explicit seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param seed update seed
   * @return text summary
   */
  public String evaluate(final BytesWritable serializedSketch, final long seed) {
    if (serializedSketch == null) { return null; }
    final CpcSketch sketch = CpcSketch.heapify(Memory.wrap(serializedSketch.getBytes()), seed);
    return sketch.toString();
  }

  /**
   * Get a summary of a given CpcSketch using the default seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @return text summary
   */
  public String evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_UPDATE_SEED);
  }

}
