/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;

@Description(name = "GetK", value = "_FUNC_(sketch)",
extended = " Returns parameter K from a given DoublesSketch sketch.")
@SuppressWarnings("javadoc")
public class GetKFromDoublesSketchUDF extends UDF {

  /**
   * Returns parameter K from a given sketch
   * @param serializedSketch serialized sketch
   * @return parameter K
   */
  public Integer evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final DoublesSketch sketch = DoublesSketch.wrap(Memory.wrap(serializedSketch.getBytes()));
    return sketch.getK();
  }

}
