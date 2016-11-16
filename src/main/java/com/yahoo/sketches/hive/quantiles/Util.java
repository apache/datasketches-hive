/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.ArrayList;
import java.util.List;

final class Util {

  static double[] objectsToPrimitives(final Double[] array) {
    final double[] result = new double[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i];
    }
    return result;
  }

  static List<Double> primitivesToList(final double[] array) {
    final List<Double> result = new ArrayList<Double>(array.length);
    for (double item: array) { result.add(item); }
    return result;
  }

}
