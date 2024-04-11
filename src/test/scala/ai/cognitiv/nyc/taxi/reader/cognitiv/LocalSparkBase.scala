package ai.cognitiv.nyc.taxi.reader.cognitiv

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import org.mockito.scalatest.MockitoSugar

trait LocalSparkBase extends LocalBase with SharedSparkContext with DatasetSuiteBase
    with DatasetComparer with MockitoSugar {

}