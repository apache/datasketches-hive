/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.hll;

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
    final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
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
