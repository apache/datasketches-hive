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

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

@SuppressWarnings("javadoc")
public class GetQuantileFromDoublesSektchUDFTest {

  @Test
  public void nullSketch() {
    Double result = new GetQuantileFromDoublesSketchUDF().evaluate(null, 0);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    Double result = new GetQuantileFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0.5);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0);
  }

}
