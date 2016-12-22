package com.yahoo.sketches.hive.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

public class ArrayOfDoublesSketchToValuesUDTFTest {

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector stringInspector =
      PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("a"),
      Arrays.asList(stringInspector)
    );

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
   public void initializeNoInspectors() throws Exception {
     ObjectInspector[] inspectors = new ObjectInspector[] { };
     GenericUDTF func = new ArrayOfDoublesSketchToValuesUDTF();
     func.initialize(inspectors);
   }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentException.class)
   public void initializeTooManyInspectors() throws Exception {
     ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
     GenericUDTF func = new ArrayOfDoublesSketchToValuesUDTF();
     func.initialize(inspectors);
   }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentTypeException.class)
   public void initializeWrongCategory() throws Exception {
     ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
     GenericUDTF func = new ArrayOfDoublesSketchToValuesUDTF();
     func.initialize(inspectors);
   }

  @SuppressWarnings("deprecation")
  @Test(expectedExceptions = UDFArgumentTypeException.class)
   public void initializeWrongType() throws Exception {
     ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector };
     GenericUDTF func = new ArrayOfDoublesSketchToValuesUDTF();
     func.initialize(inspectors);
   }

  @SuppressWarnings({ "deprecation", "unchecked" })
  @Test
  public void normalCase() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDTF func = new ArrayOfDoublesSketchToValuesUDTF();
    ObjectInspector resultInspector = func.initialize(inspectors);
    checkResultInspector(resultInspector);
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch.update(1, new double[] {1, 2});
    sketch.update(2, new double[] {1, 2});
    MockCollector collector = new MockCollector();
    func.setCollector(collector);
    func.process(new Object[] {new BytesWritable(sketch.toByteArray())});
    Assert.assertEquals(collector.list.size(), 2);
    Assert.assertEquals(((Object[]) collector.list.get(0)).length, 1);
    Assert.assertEquals(((List<Double>) ((Object[]) collector.list.get(0))[0]), Arrays.asList(1.0, 2.0));
    Assert.assertEquals(((List<Double>) ((Object[]) collector.list.get(1))[0]), Arrays.asList(1.0, 2.0));
  }

  private static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.STRUCT);
    List<? extends StructField> fields = ((StructObjectInspector) resultInspector).getAllStructFieldRefs();
    Assert.assertEquals(fields.size(), 1);
    Assert.assertEquals(fields.get(0).getFieldObjectInspector().getCategory(), ObjectInspector.Category.LIST);
    Assert.assertEquals(
        ((PrimitiveObjectInspector) ((ListObjectInspector) fields.get(0).getFieldObjectInspector()).getListElementObjectInspector()).getPrimitiveCategory(),
        PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
      );
  }

  private static class MockCollector implements Collector {
    List<Object> list = new ArrayList<Object>();

    @Override
    public void collect(Object object) throws HiveException {
      list.add(object);
    }

  }

}
