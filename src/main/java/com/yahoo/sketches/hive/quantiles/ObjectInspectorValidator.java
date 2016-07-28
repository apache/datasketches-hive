package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

final class ObjectInspectorValidator {

  static void validateCategoryPrimitive(final ObjectInspector inspector, final int index) throws SemanticException {
    if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(index, "Primitive argument expected, but "
          + inspector.getCategory().name() + " was recieved");
    }
  }

  static void validateGivenPrimitiveCategory(final ObjectInspector inspector, final int index,
      final PrimitiveObjectInspector.PrimitiveCategory category) throws SemanticException
  {
    validateCategoryPrimitive(inspector, index);
    final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
    if (primitiveInspector.getPrimitiveCategory() != category) {
      throw new UDFArgumentTypeException(index, category.name() + " value expected as the argument "
          + (index + 1) + " but " + primitiveInspector.getPrimitiveCategory().name() + " was received");
    }
  }

}
