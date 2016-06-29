/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import com.yahoo.sketches.ArrayOfStringsSerDe;

@Description(name = "Merge", value = "_FUNC_(sketch) - "
    + "Returns an ItemsSketch<String> in a serialized form as a binary blob."
    + " Input values must also be serialized sketches.")
public class UnionStringsSketchUDAF extends UnionItemsSketchUDAF<String> {

  @Override
  GenericUDAFEvaluator createEvaluator() {
    return new UnionStringsSketchEvaluator();
  }

  static class UnionStringsSketchEvaluator extends UnionEvaluator<String> {

    UnionStringsSketchEvaluator() {
      super(Comparator.naturalOrder(), new ArrayOfStringsSerDe());
    }

  }

}
