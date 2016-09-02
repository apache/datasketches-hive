/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Summary;

abstract class State<S extends Summary> extends AbstractAggregationBuffer {

  private int nominalNumEntries_;

  void init(final int nominalNumEntries) {
    nominalNumEntries_ = nominalNumEntries;
  }

  int getNominalNumEntries() {
    return nominalNumEntries_;
  }

  abstract Sketch<S> getResult();

  abstract void reset();

}
