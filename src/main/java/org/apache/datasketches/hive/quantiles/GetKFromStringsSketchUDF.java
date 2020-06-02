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

package org.apache.datasketches.hive.quantiles;

import java.util.Comparator;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "GetK", value = "_FUNC_(sketch)",
extended = " Returns parameter K from a given ItemsSketch<String> sketch.")
@SuppressWarnings("javadoc")
public class GetKFromStringsSketchUDF extends UDF {

  /**
   * Returns parameter K from a given sketch
   * @param serializedSketch serialized sketch
   * @return parameter K
   */
  public Integer evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      BytesWritableHelper.wrapAsMemory(serializedSketch),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return sketch.getK();
  }

}
