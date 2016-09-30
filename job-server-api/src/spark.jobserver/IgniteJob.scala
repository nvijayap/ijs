package spark.jobserver

import com.typesafe.config.Config
import org.apache.ignite.spark.IgniteContext
import org.scalactic._

import spark.jobserver.api.{IgniteJobBase => NewIgniteJob, JobEnvironment, ValidationProblem}

sealed trait IgniteJobValidation {
  // NOTE(harish): We tried using lazy eval here by passing in a function
  // instead, which worked fine with tests but when run with the job-server
  // it would just hang and timeout. This is something worth investigating
  def &&(sparkValidation: IgniteJobValidation): IgniteJobValidation = this match {
    case IgniteJobValid => sparkValidation
    case x => x
  }
}
case object IgniteJobValid extends IgniteJobValidation
case class IgniteJobInvalid(reason: String) extends IgniteJobValidation with ValidationProblem

/**
 *  This is the deprecated trait is the main API for Ignite jobs submitted to the Job Server.
 */
@Deprecated
trait IgniteJobBase extends NewIgniteJob {
  type JobOutput = Any
  type JobData = Config

  var namedObjects: NamedObjects = null

  def runJob(sc: C, runtime: JobEnvironment, data: JobData): JobOutput = runJob(sc, data)

  def validate(sc: C, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
    namedObjects = runtime.namedObjects
    validate(sc, config) match {
      case IgniteJobValid      => Good(config)
      case i: IgniteJobInvalid => Bad(One(i))
    }
  }

  /**
   * This is the entry point for a Ignite Job Server to execute Ignite jobs.
   * This function should create or reuse RDDs and return the result at the end, which the
   * Job Server will cache or display.
   * @param sc a IgniteContext or similar for the job.  May be reused across jobs.
   * @param jobConfig the Typesafe Config object passed into the job request
   * @return the job result
   */
  def runJob(sc: C, jobConfig: Config): Any

  /**
   * This method is called by the job server to allow jobs to validate their input and reject
   * invalid job requests.  If IgniteJobInvalid is returned, then the job server returns 400
   * to the user.
   * NOTE: this method should return very quickly.  If it responds slowly then the job server may time out
   * trying to start this job.
   * @return either IgniteJobValid or IgniteJobInvalid
   */
  def validate(sc: C, config: Config): IgniteJobValidation
}

trait IgniteJob extends IgniteJobBase {
  type C = IgniteContext
}
