/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

class State extends AbstractAggregationBuffer {

  private int lgK_;
  private TgtHllType type_;
  private Union union_;

  void init(final int lgK, final TgtHllType type) {
    lgK_ = lgK;
    type_ = type;
    union_ = new Union(lgK);
  }

  int getLgK() {
    return lgK_;
  }

  TgtHllType getType() {
    return type_;
  }

  boolean isInitialized() {
    return union_ != null;
  }

  void update(final Object data, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
      case BINARY:
        union_.update(PrimitiveObjectInspectorUtils.getBinary(data, objectInspector)
            .getBytes());
        return;
      case BYTE:
        union_.update(PrimitiveObjectInspectorUtils.getByte(data, objectInspector));
        return;
      case DOUBLE:
        union_.update(PrimitiveObjectInspectorUtils.getDouble(data, objectInspector));
        return;
      case FLOAT:
        union_.update(PrimitiveObjectInspectorUtils.getFloat(data, objectInspector));
        return;
      case INT:
        union_.update(PrimitiveObjectInspectorUtils.getInt(data, objectInspector));
        return;
      case LONG:
        union_.update(PrimitiveObjectInspectorUtils.getLong(data, objectInspector));
        return;
      case STRING:
        // conversion to char[] avoids costly UTF-8 encoding
        union_.update(PrimitiveObjectInspectorUtils.getString(data, objectInspector)
            .toCharArray());
        return;
      case CHAR:
        union_.update(PrimitiveObjectInspectorUtils.getHiveChar(data, objectInspector)
            .getValue().toCharArray());
        return;
      case VARCHAR:
        union_.update(PrimitiveObjectInspectorUtils.getHiveVarchar(data, objectInspector)
            .getValue().toCharArray());
        return;
      default:
        throw new IllegalArgumentException(
          "Unrecongnized input data type " + data.getClass().getSimpleName() + " category "
          + objectInspector.getPrimitiveCategory() + ", please use data of the following types: "
          + "byte, double, float, int, long, char, varchar or string.");
    }
  }

  void update(final HllSketch sketch) {
    union_.update(sketch);
  }

  HllSketch getResult() {
    if (union_ == null) { return null; }
    return union_.getResult(getType());
  }

  void reset() {
    union_ = null;
  }

}
