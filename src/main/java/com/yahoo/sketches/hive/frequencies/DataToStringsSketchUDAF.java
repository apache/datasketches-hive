/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */
package com.yahoo.sketches.hive.frequencies;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.yahoo.sketches.ArrayOfStringsSerDe;

@Description(name = "DataToSketch", value = "_FUNC_(value, maxMapSize) - "
    + "Returns an ItemsSketch<String> in a serialized form as a binary blob."
    + " Values must be of string type."
    + " Parameter maxMapSize controls the accuracy and the size of the sketch.")
public class DataToStringsSketchUDAF extends DataToItemsSketchUDAF<String> {

  @Override
  GenericUDAFEvaluator createEvaluator() {
    return new DataToStringsSketchEvaluator();
  }

  static class DataToStringsSketchEvaluator extends DataToItemsSketchEvaluator<String> {

    DataToStringsSketchEvaluator() {
      super(new ArrayOfStringsSerDe());
    }

    @Override
    String extractValue(Object data, ObjectInspector objectInspector) throws HiveException {
      Object value = inputObjectInspector.getPrimitiveJavaObject(data);
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof HiveChar) {
        return ((HiveChar) value).getValue();
      } else if (value instanceof HiveVarchar) {
        return ((HiveVarchar) value).getValue();
      } else {
        throw new UDFArgumentTypeException(0, "unsupported type " + value.getClass().getName());
      }
    }

  }

}
