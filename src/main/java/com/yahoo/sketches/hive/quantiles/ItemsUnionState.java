/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;

class ItemsUnionState<T> extends AbstractAggregationBuffer {

  private int k_;
  private final Comparator<? super T> comparator_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsUnion<T> union;

  ItemsUnionState(final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    comparator_ = comparator;
    serDe_ = serDe;
  }

  // initializing k is needed for building sketches using update(value)
  // not needed for merging sketches using update(sketch)
  void init(final int k) {
    k_ = k;
    buildUnion();
  }

  boolean isInitialized() {
    return union != null;
  }

  void update(final T value) {
    union.update(value);
  }

  void update(final byte[] serializedSketch) {
    if (union == null) buildUnion();
    union.update(ItemsSketch.getInstance(new NativeMemory(serializedSketch), comparator_, serDe_));
  }

  public ItemsSketch<T> getResult() {
    if (union == null) return null;
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

  private void buildUnion() {
    if (k_ > 0) {
      union = ItemsUnion.getInstance(k_, comparator_);
    } else {
      union = ItemsUnion.getInstance(comparator_);
    }
  }
}
