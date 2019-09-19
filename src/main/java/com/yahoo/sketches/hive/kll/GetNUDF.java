/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

@Description(name = "GetN", value = "_FUNC_(sketch)",
extended = " Returns the total number of observed input values (stream length) from a given KllFloatsSketch.")
@SuppressWarnings("javadoc")
public class GetNUDF extends UDF {

  /**
   * Returns N from a given sketch
   * @param serializedSketch serialized sketch
   * @return stream length
   */
  public Long evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    return sketch.getN();
  }

}
