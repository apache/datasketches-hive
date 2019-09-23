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

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToNumberOfRetainedEntriesUDFTest {

  @Test
  public void nullSketch() {
    Integer result = new ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    Integer result = new ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals((int) result, 0);
  }

  @Test
  public void normalCase() {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch.update(1, new double[] {0});
    Integer result = new ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF().evaluate(new BytesWritable(sketch.compact().toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals((int) result, 1);
  }

}
