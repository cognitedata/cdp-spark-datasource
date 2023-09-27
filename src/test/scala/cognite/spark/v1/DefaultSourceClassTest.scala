package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers}


class DefaultSourceClassTest extends FlatSpec with Matchers {
  it should "have default constructor" in {
    // Data source should be constructable with no arguments and
    // any parameters should be parsed within specific methods.
    // Implicit parameters are also not allowed.
    // Otherwise spark executors will fail to create it.
    val res = new DefaultSource()
    res shouldBe a[DefaultSource]
  }

  it should "really have a no-argument constructor" in {
    // let's also check via reflection to rule out varargs option like A(String... s)
    // getConstructor() will only pick up no-args version
    // getConstructor(String[].class) can be used to select varargs version
    // reflection also makes sure no scala implicits are needed
    val res = classOf[DefaultSource].getConstructor().newInstance()
    res shouldBe a[DefaultSource]
  }
}
