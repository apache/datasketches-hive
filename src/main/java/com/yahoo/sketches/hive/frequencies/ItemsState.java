/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.frequencies;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.frequencies.ItemsSketch;

class ItemsState<T> extends AbstractAggregationBuffer {

  private int maxMapSize_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsSketch<T> sketch;

  ItemsState(final ArrayOfItemsSerDe<T> serDe) {
    serDe_ = serDe;
  }

  // initializing maxMapSize is needed for building sketches using update(value)
  // not needed for merging sketches using update(sketch)
  void init(final int maxMapSize) {
    maxMapSize_ = maxMapSize;
    sketch = new ItemsSketch<T>(maxMapSize_);
  }

  boolean isInitialized() {
    return sketch != null;
  }

  void update(final T value) {
    sketch.update(value);
  }

  void update(final byte[] serializedSketch) {
    final ItemsSketch<T> incomingSketch = ItemsSketch.getInstance(Memory.wrap(serializedSketch), serDe_);
    if (sketch == null) {
      sketch = incomingSketch;
    } else {
      sketch.merge(incomingSketch);
    }
  }

  public ItemsSketch<T> getResult() {
    return sketch;
  }

  void reset() {
    sketch = null;
  }

}
