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

  private int k_;
  private DoublesUnion union;

  // initializing k is needed for building sketches using update(double)
  // not needed for merging sketches using update(sketch)
  void init(final int k) {
    this.k_ = k;
    buildUnion();
  }

  boolean isInitialized() {
    return union != null;
  }

  void update(final double value) {
    union.update(value);
  }

  void update(final byte[] serializedSketch) {
    if (union == null) buildUnion();
    union.update(new NativeMemory(serializedSketch));
  }

  public DoublesSketch getResult() {
    if (union == null) return null;
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

  private void buildUnion() {
    final DoublesUnionBuilder unionBuilder = DoublesUnion.builder();
    if (k_ > 0) unionBuilder.setK(k_);
    union = unionBuilder.build();
  }

}
