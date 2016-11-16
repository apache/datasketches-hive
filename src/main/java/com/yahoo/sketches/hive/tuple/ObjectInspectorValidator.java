/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

class ObjectInspectorValidator {

  static void validateCategoryPrimitive(final ObjectInspector inspector, final int index)
      throws UDFArgumentTypeException {
    if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(index, "Primitive parameter expected, but "
          + inspector.getCategory().name() + " was recieved as parameter " + (index + 1));
    }
  }

  static void validateGivenPrimitiveCategory(final ObjectInspector inspector, final int index,
      final PrimitiveObjectInspector.PrimitiveCategory category) throws UDFArgumentTypeException
  {
    validateCategoryPrimitive(inspector, index);
    final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
    if (primitiveInspector.getPrimitiveCategory() != category) {
      throw new UDFArgumentTypeException(index, category.name() + " value expected as parameter "
          + (index + 1) + " but " + primitiveInspector.getPrimitiveCategory().name() + " was received");
    }
  }

  static void validateIntegralParameter(final ObjectInspector inspector, final int index)
      throws UDFArgumentTypeException {
    validateCategoryPrimitive(inspector, index);
    PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
    switch (primitiveInspector.getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      break;
    // all other types are invalid
    default:
      throw new UDFArgumentTypeException(index, "Only integral type parameters are expected but "
          + primitiveInspector.getPrimitiveCategory().name() + " was passed as parameter " + (index + 1));
    }
  }

}
