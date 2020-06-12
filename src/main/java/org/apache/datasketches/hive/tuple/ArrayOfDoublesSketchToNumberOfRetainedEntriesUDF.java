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
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "ArrayOfDoublesSketchToNumberOfRetainedEntries",
    value = "_FUNC_(sketch)",
    extended = "Returns the number of retained entries from a given ArrayOfDoublesSketch."
    + " The result is an integer value.")
@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF extends UDF {

  /**
   * Get number of retained entries from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return number of retained entries
   */
  public Integer evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        BytesWritableHelper.wrapAsMemory(serializedSketch));
    return sketch.getRetainedEntries();
  }

}
