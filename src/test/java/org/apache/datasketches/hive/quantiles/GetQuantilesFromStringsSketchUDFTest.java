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
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfLongsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.quantiles.ItemsSketch;

@SuppressWarnings("javadoc")
public class GetQuantilesFromStringsSketchUDFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(null, 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfFractions() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void fractionsNormalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0.0, 0.5, 1.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), "a");
    Assert.assertEquals(result.get(1), "b");
    Assert.assertEquals(result.get(2), "c");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void evenlySpacedZero() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    new GetQuantilesFromStringsSketchUDF()
      .evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0);
  }

  @Test
  public void evenlySpacedEmptySketch() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 1);
    Assert.assertNull(result);
  }

  @Test
  public void evenlySpacedNormalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 3);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), "a");
    Assert.assertEquals(result.get(1), "b");
    Assert.assertEquals(result.get(2), "c");
  }

  //Note: this exception is only caught because a bounds error was detected.
  //If a bounds error is not detected from a wrong type assignment, unexpected results could occur.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void fractionsWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantilesFromStringsSketchUDF() //WRONG SKETCH
      .evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 0.5);
  }

  //Note: this exception is only caught because a bounds error was detected.
  //If a bounds error is not detected from a wrong type assignment, unexpected results could occur.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void evenlySpacedWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantilesFromStringsSketchUDF() //WRONG SKETCH
      .evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 1);
  }

}
