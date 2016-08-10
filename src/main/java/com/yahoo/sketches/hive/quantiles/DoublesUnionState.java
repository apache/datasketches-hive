/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;

class DoublesUnionState extends AbstractAggregationBuffer {

  private DoublesUnion union;

  // initializing is needed only in the first phase (iterate)
  void init(final int k) {
    final DoublesUnionBuilder unionBuilder = DoublesUnion.builder();
    if (k > 0) unionBuilder.setK(k);
    union = unionBuilder.build();
  }

  boolean isInitialized() {
    return union != null;
  }

  void update(final double value) {
    if (union == null) union = DoublesUnion.builder().build();
    union.update(value);
  }

  void update(final byte[] serializedSketch) {
    final DoublesSketch incomingSketch = DoublesSketch.heapify(new NativeMemory(serializedSketch));
    if (union == null) {
      union = DoublesUnionBuilder.build(incomingSketch);
    } else {
      union.update(incomingSketch);
    }
  }

  public DoublesSketch getResult() {
    if (union == null) return null;
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

}
