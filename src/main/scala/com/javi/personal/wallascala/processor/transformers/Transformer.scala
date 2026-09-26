package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.DataFrame

trait Transformer[In, Out] {
  def transform(input: In): Out
}

trait SingleInputTransformer extends Transformer[DataFrame, DataFrame]

trait TwoInputTransformer[In1, In2] extends Transformer[(In1, In2), DataFrame] {
  def transform(in1: In1, in2: In2): DataFrame = transform((in1, in2))
}
