package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import com.yahoo.sketches.quantiles.Union;
import com.yahoo.sketches.quantiles.UnionBuilder;

class QuantilesUnionState extends AbstractAggregationBuffer {

  private int k_;
  private Union union;

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

  public QuantilesSketch getResult() {
    if (union == null) return null;
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

  private void buildUnion() {
    final UnionBuilder unionBuilder = Union.builder();
    if (k_ > 0) unionBuilder.setK(k_);
    union = unionBuilder.build();
  }
}
