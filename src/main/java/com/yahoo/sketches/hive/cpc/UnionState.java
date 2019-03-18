/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import com.yahoo.sketches.cpc.CpcSketch;
import com.yahoo.sketches.cpc.CpcUnion;

class UnionState extends State {

  private CpcUnion union_;

  @Override
  boolean isInitialized() {
    return union_ != null;
  }

  @Override
  void init(final int lgK, final long seed) {
    super.init(lgK, seed);
    union_ = new CpcUnion(lgK, seed);
  }

  void update(final CpcSketch sketch) {
    union_.update(sketch);
  }

  @Override
  CpcSketch getResult() {
    if (union_ == null) { return null; }
    return union_.getResult();
  }

  @Override
  void reset() {
    union_ = null;
  }

}
