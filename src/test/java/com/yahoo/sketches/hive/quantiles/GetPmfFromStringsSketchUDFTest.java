/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

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
