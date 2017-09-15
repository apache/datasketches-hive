/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchToNumberOfRetainedEntries",
    value = "_FUNC_(sketch)",
    extended = "Returns the number of retained entries from a given ArrayOfDoublesSketch."
    + " The result is an integer value.")
public class ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF extends UDF {

  /**
   * Get number of retained entries from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return number of retained entries
   */
  public Integer evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        Memory.wrap(serializedSketch.getBytes()));
    return sketch.getRetainedEntries();
  }

}
