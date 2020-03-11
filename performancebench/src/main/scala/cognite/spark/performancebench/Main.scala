package cognite.spark.performancebench

import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val eventPerf = new EventPerformance()
    eventPerf.run()
    sys.exit(0)
    IO.pure(ExitCode.Success)
  }
}
