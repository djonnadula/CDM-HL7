package com.hca.cdm.job.psg.aco.tests

import com.hca.cdm._
import org.apache.log4j.PropertyConfigurator._

/**
  * Created by dof7475 on 7/24/2017.
  */
object PSGACOADTTestUtils {

  def loadProperties() : Unit =  {
    configure(currThread.getContextClassLoader.getResource("jobs-test-log4j.properties"))
  }

}
