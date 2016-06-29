/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.ItemsSketch;

import org.testng.annotations.Test;
import org.testng.Assert;

public class GetQuantileFromStringsSketchUDFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    String result = new GetQuantileFromStringsSketchUDF().evaluate(null, 0);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    String result = new GetQuantileFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0.5);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, "b");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void fractionsWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantileFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 0.5);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void evenlySpacedWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantileFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 0.5);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void evenlySpacedZero() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0);
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

}
