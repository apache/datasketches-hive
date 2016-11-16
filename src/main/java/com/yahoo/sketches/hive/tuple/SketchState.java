/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.UpdatableSummary;

class SketchState<U, S extends UpdatableSummary<U>> extends State<S> {

  private UpdatableSketch<U, S> sketch_;

  boolean isInitialized() {
    return sketch_ != null;
  }

  void init(int nominalNumEntries, float samplingProbability, final SummaryFactory<S> summaryFactory) {
    super.init(nominalNumEntries);
    sketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory).setNominalEntries(nominalNumEntries)
        .setSamplingProbability(samplingProbability).build();
  }

  void update(Object data, PrimitiveObjectInspector keyObjectInspector, U value) {
    switch (keyObjectInspector.getPrimitiveCategory()) {
    case BINARY:
      sketch_.update(PrimitiveObjectInspectorUtils.getBinary(data, keyObjectInspector).getBytes(), value);
      return;
    case BYTE:
      sketch_.update(PrimitiveObjectInspectorUtils.getByte(data, keyObjectInspector), value);
      return;
    case DOUBLE:
      sketch_.update(PrimitiveObjectInspectorUtils.getDouble(data, keyObjectInspector), value);
      return;
    case FLOAT:
      sketch_.update(PrimitiveObjectInspectorUtils.getFloat(data, keyObjectInspector), value);
      return;
    case INT:
      sketch_.update(PrimitiveObjectInspectorUtils.getInt(data, keyObjectInspector), value);
      return;
    case LONG:
      sketch_.update(PrimitiveObjectInspectorUtils.getLong(data, keyObjectInspector), value);
      return;
    case STRING:
      sketch_.update(PrimitiveObjectInspectorUtils.getString(data, keyObjectInspector), value);
      return;
    default:
      throw new IllegalArgumentException(
          "Unrecongnized input data type, please use data of type: "
      + "byte, double, float, int, long, or string only.");
    }
  }

  @Override
  Sketch<S> getResult() {
    if (sketch_ == null) { return null; }
    // assumes that it is called once at the end of processing
    // since trimming to nominal number of entries is expensive
    sketch_.trim();
    return sketch_.compact();
  }

  @Override
  void reset() {
    sketch_ = null;
  }

}
