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
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import org.testng.annotations.Test;
import org.testng.Assert;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.quantiles.ItemsSketch;

@SuppressWarnings("javadoc")
public class GetPmfFromStringsSketchUDFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    List<Double> result = new GetPmfFromStringsSketchUDF().evaluate(null, "");
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfSplitPoints() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<Double> result = new GetPmfFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1.0);
  }

  @Test
  public void emptySketch() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    List<Double> result = new GetPmfFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), "a");
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    List<Double> result = new GetPmfFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), "a", "c", "e");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.5);
    Assert.assertEquals(result.get(2), 0.5);
    Assert.assertEquals(result.get(3), 0.0);
  }

}
