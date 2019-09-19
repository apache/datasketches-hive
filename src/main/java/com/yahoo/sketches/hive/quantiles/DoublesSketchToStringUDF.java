/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;

@Description(name = "ToString", value = "_FUNC_(sketch)",
extended = " Returns a human-readable summary of a given DoublesSketch.")
@SuppressWarnings("javadoc")
public class DoublesSketchToStringUDF extends UDF {

  /**
   * Returns a human-readable summary of a given sketch
   * @param serializedSketch serialized sketch
   * @return text summary
   */
  public String evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final DoublesSketch sketch = DoublesSketch.wrap(Memory.wrap(serializedSketch.getBytes()));
    return sketch.toString();
  }

}
