/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

abstract class ArrayOfDoublesState extends AbstractAggregationBuffer {

  private int numNominalEntries_;
  private int numValues_;

  void init(final int numNominalEntries, final int numValues) {
    numNominalEntries_ = numNominalEntries;
    numValues_ = numValues;
  }

  int getNumNominalEntries() {
    return numNominalEntries_;
  }

  int getNumValues() {
    return numValues_;
  }

  abstract ArrayOfDoublesSketch getResult();

  abstract void reset();

}
