/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.cpc.CpcSketch;

abstract class State extends AbstractAggregationBuffer {

  private int lgK_;
  private long seed_;

  void init(final int lgK, final long seed) {
    lgK_ = lgK;
    seed_ = seed;
  }

  int getLgK() {
    return lgK_;
  }

  long getSeed() {
    return seed_;
  }

  abstract boolean isInitialized();

  abstract CpcSketch getResult();

  abstract void reset();

}
