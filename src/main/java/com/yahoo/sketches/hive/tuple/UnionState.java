/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.Union;

class UnionState<S extends Summary> extends State<S> {

  private Union<S> union_;

  boolean isInitialized() {
    return union_ != null;
  }

  @Override
  void init(final int numNominalEntries, final SummaryFactory<S> summaryFactory) {
    super.init(numNominalEntries, summaryFactory);
    union_ = new Union<S>(numNominalEntries, summaryFactory);
  }

  void update(final Sketch<S> sketch) {
    union_.update(sketch);
  }

  @Override
  Sketch<S> getResult() {
    if (union_ == null) return null;
    return union_.getResult();
  }

  @Override
  void reset() {
    union_ = null;    
  }

}
