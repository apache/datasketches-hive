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

package org.apache.datasketches.hive.cpc;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class SketchToStringUDFTest {

  @Test
  public void nullSketch() {
    final String result = new SketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final CpcSketch sketch = new CpcSketch();
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCase() {
    final CpcSketch sketch = new CpcSketch();
    sketch.update(1);
    sketch.update(2);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void customSeed() {
    final long seed = 123;
    final CpcSketch sketch = new CpcSketch(12, seed);
    sketch.update(1);
    sketch.update(2);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()), seed);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
