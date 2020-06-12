/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.tuple;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "ArrayOfDoublesSketchToQuantilesSketch",
    value = "_FUNC_(sketch, column, k)",
    extended = "Returns a quanitles DoublesSketch constructed from a given"
    + " column of double values from a given ArrayOfDoublesSketch using parameter k"
    + " that determines the accuracy and size of the quantiles sketch."
    + " The column number is optional (the default is 1)."
    + " The parameter k is optional (the default is defined in the sketch library)."
    + " The result is a serialized quantiles sketch.")
@SuppressWarnings("javadoc")
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
        BytesWritableHelper.wrapAsMemory(serializedSketch));
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
