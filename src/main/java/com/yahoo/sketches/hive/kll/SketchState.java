/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.kll.KllFloatsSketch;

class SketchState extends AbstractAggregationBuffer {

  private KllFloatsSketch state_;

  // initialization is needed in the first phase (iterate) only
  void init() {
    state_ = new KllFloatsSketch();
  }

  void init(final int k) {
    state_ = new KllFloatsSketch(k);
  }

  boolean isInitialized() {
    return state_ != null;
  }

  void update(final float value) {
    state_.update(value);
  }

  void update(final KllFloatsSketch sketch) {
    if (state_ == null) {
      state_ = sketch;
    } else {
      state_.merge(sketch);
    }
  }

  public KllFloatsSketch getResult() {
    return state_;
  }

  void reset() {
    state_ = null;
  }

}
