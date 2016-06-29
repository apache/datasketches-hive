package com.yahoo.sketches.hive.quantiles;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.ItemsSketch;

import org.testng.annotations.Test;
import org.testng.Assert;

public class DataToStringsSketchUDAFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  static final ObjectInspector stringInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);

  static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, stringInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
    Arrays.asList("a"),
    Arrays.asList(intInspector)
  );

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test
  public void iterateTerminatePartial() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToStringsSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    checkResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    ItemsUnionState<String> state = (ItemsUnionState<String>) eval.getNewAggregationBuffer();
    eval.iterate(state, new Object[] { new org.apache.hadoop.io.Text("a"), new IntWritable(256) });
    eval.iterate(state, new Object[] { new org.apache.hadoop.io.Text("b"), new IntWritable(256) });

    BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
    ItemsSketch<String> resultSketch = ItemsSketch.getInstance(new NativeMemory(bytes.getBytes()), comparator, serDe);
    Assert.assertEquals(resultSketch.getK(), 256);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), "a");
    Assert.assertEquals(resultSketch.getMaxValue(), "b");
    eval.close();
  }

  @Test
  public void mergeTerminate() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToStringsSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] { binaryInspector });
    checkResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    ItemsUnionState<String> state = (ItemsUnionState<String>) eval.getNewAggregationBuffer();
    state.init(256);
    state.update("a");

    ItemsSketch<String> sketch = ItemsSketch.getInstance(256, comparator);
    sketch.update("b");

    eval.merge(state, new BytesWritable(sketch.toByteArray(serDe)));

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    ItemsSketch<String> resultSketch = ItemsSketch.getInstance(new NativeMemory(bytes.getBytes()), comparator, serDe);
    Assert.assertEquals(resultSketch.getK(), 256);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), "a");
    Assert.assertEquals(resultSketch.getMaxValue(), "b");
    eval.close();
  }

  private static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) resultInspector).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.BINARY
    );
  }

}
