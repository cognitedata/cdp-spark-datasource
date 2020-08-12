package cognite.spark.jackson

import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.spark.sql.Row

private[spark] object SparkModule extends SimpleModule {
  addSerializer(classOf[Row], new RowSerializer)
}
