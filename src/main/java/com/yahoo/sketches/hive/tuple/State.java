/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryFactory;

abstract class State<S extends Summary> extends AbstractAggregationBuffer {

  private int numNominalEntries_;
  private SummaryFactory<S> summaryFactory_;

  void init(final int numNominalEntries, final SummaryFactory<S> summaryFactory) {
    numNominalEntries_ = numNominalEntries;
    summaryFactory_ = summaryFactory;
  }

  int getNumNominalEntries() {
    return numNominalEntries_;
  }

  SummaryFactory<S> getSummaryFactory() {
    return summaryFactory_;
  }

  abstract Sketch<S> getResult();

  abstract void reset();

}
