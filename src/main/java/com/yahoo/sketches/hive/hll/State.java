/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

abstract class State extends AbstractAggregationBuffer {

  private int lgK_;
  private TgtHllType type_;

  void init(final int lgK, final TgtHllType type) {
    lgK_ = lgK;
    type_ = type;
  }

  int getLgK() {
    return lgK_;
  }

  TgtHllType getType() {
    return type_;
  }

  abstract boolean isInitialized();

  abstract void update(final Object data, final PrimitiveObjectInspector keyObjectInspector);

  abstract HllSketch getResult();

  abstract void reset();

}
