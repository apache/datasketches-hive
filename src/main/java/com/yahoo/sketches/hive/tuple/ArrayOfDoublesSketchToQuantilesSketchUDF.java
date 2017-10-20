/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchToQuantilesSketch",
    value = "_FUNC_(sketch, column, k)",
    extended = "Returns a quanitles DoublesSketch constructed from a given"
    + " column of double values from a given ArrayOfDoublesSketch using parameter k"
    + " that determines the accuracy and size of the quantiles sketch."
    + " The column number is optional (the default is 1)."
    + " The parameter k is optional (the default is defined in the sketch library)."
    + " The result is a serialized quantiles sketch.")
public class ArrayOfDoublesSketchToQuantilesSketchUDF extends UDF {

  /**
   * Convert the first column from a given ArrayOfDoublesSketch to a quantiles DoublesSketch
   * using the default parameter k.
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return serialized DoublesSketch
   */
  public BytesWritable evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, 1, 0);
  }

  /**
   * Convert a given column from a given ArrayOfDoublesSketch to a quantiles DoublesSketch
   * using the default parameter k.
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @param column 1-based column number
   * @return serialized DoublesSketch
   */
  public BytesWritable evaluate(final BytesWritable serializedSketch, final int column) {
    return evaluate(serializedSketch, column, 0);
  }

  /**
   * Convert a given column from a given ArrayOfDoublesSketch to a quantiles DoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @param column 1-based column number
   * @param k parameter that determines the accuracy and size of the quantiles sketch
   * @return serialized DoublesSketch
   */
  public BytesWritable evaluate(final BytesWritable serializedSketch, final int column,
      final int k) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        Memory.wrap(serializedSketch.getBytes()));
    if (column < 1) {
      throw new IllegalArgumentException("Column number must be greater than zero. Received: "
          + column);
    }
    if (column > sketch.getNumValues()) {
      throw new IllegalArgumentException("Column number " + column
          + " is out of range. The given sketch has "
          + sketch.getNumValues() + " columns");
    }
    final DoublesSketchBuilder builder = DoublesSketch.builder();
    if (k > 0) {
      builder.setK(k);
    }
    final UpdateDoublesSketch qs = builder.build();
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getValues()[column - 1]);
    }
    return new BytesWritable(qs.compact().toByteArray());
  }

}
