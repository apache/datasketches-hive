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

package org.apache.datasketches.hive.hll;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.hll.HllSketch;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "SketchToString",
    value = "_FUNC_(sketch)",
    extended = "Returns a human-readable summary of a given HllSketch.")
@SuppressWarnings("deprecation")
public class SketchToStringUDF extends UDF {

  /**
   * Get a human-readable summary of a given HllSketch
   * @param serializedSketch HllSketch in a serialized binary form
   * @return text summary
   */
  public String evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final HllSketch sketch = HllSketch.wrap(BytesWritableHelper.wrapAsMemory(serializedSketch));
    return sketch.toString();
  }

}
