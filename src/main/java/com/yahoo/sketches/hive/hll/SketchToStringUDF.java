/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;

@Description(
    name = "SketchToString",
    value = "_FUNC_(sketch)",
    extended = "Returns an human-readable summary of a given HllSketch.")
@SuppressWarnings("javadoc")
public class SketchToStringUDF extends UDF {

  /**
   * Get a human-readable summary of a given HllSketch
   * @param serializedSketch HllSketch in a serialized binary form
   * @return text summary
   */
  public String evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final HllSketch sketch = HllSketch.wrap(Memory.wrap(serializedSketch.getBytes()));
    return sketch.toString();
  }

}
