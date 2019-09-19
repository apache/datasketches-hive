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

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.hll.HllSketch;

@SuppressWarnings("javadoc")
public class SketchToEstimateUDFTest {

  @Test
  public void nullSketch() {
    Double result = new SketchToEstimateUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    HllSketch sketch = new HllSketch(10);
    Double result = new SketchToEstimateUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 0.0);
  }

  @Test
  public void normalCase() {
    HllSketch sketch = new HllSketch(10);
    sketch.update(1);
    sketch.update(2);
    Double result = new SketchToEstimateUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0, 0.01);
  }

}
