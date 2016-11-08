/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;

class ItemsUnionState<T> extends AbstractAggregationBuffer {

  private final Comparator<? super T> comparator_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsUnion<T> union;

  ItemsUnionState(final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    comparator_ = comparator;
    serDe_ = serDe;
  }

  // initializing is needed only in the first phase (iterate)
  void init(final int k) {
    if (k > 0) {
      union = ItemsUnion.getInstance(k, comparator_);
    } else {
      union = ItemsUnion.getInstance(comparator_);
    }
  }

  boolean isInitialized() {
    return union != null;
  }

  void update(final T value) {
    if (union == null) union = ItemsUnion.getInstance(comparator_);
    union.update(value);
  }

  void update(final byte[] serializedSketch) {
    final ItemsSketch<T> incomingSketch = ItemsSketch.getInstance(new NativeMemory(serializedSketch), comparator_, serDe_);
    if (union == null) {
      union = ItemsUnion.getInstance(incomingSketch);
    } else {
      union.update(incomingSketch);
    }
  }

  public ItemsSketch<T> getResult() {
    if (union == null) return null;
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

}
