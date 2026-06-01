/** VSIO - Veitangie's Simple IO Monad Copyright (C) 2026 Veitangie
  *
  * This program is free software: you can redistribute it and/or modify it under the terms of the
  * GNU General Public License as published by the Free Software Foundation, either version 3 of the
  * License, or (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
  * the GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License along with this program. If
  * not, see <https://www.gnu.org/licenses/>.
  */

package dev.veitangie.vsio

import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.ExecutionContext

object VSIOLoadTestSuite {

  // 1. The Daemon Thread Fix
  // This factory forces the CachedThreadPool to create daemon threads.
  // When the main thread finishes, the JVM will instantly terminate instead of
  // waiting 60 seconds for the cached threads to time out.
  private val daemonFactory = {
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setDaemon(true)
        t
      }
    }
  }

  // Inject the daemonized blocking pool
  import scala.concurrent.ExecutionContext.Implicits.global
  given env: SimpleExecutionContext = SimpleExecutionContext(
    computePool = global,
    computeSize = Runtime.getRuntime.availableProcessors(),
    blockingPool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(daemonFactory))
  )

  def main(args: Array[String]): Unit = {
    val iterations = 1000
    println(s"--- Starting SIO Statistical Load Tests ($iterations Iterations) ---")
    println("Hammering the scheduler to expose 1-in-a-million race conditions...")

    var failed    = false
    val startTime = System.currentTimeMillis()

    for
      i <- 1 to iterations
      if !failed
    do {
      if i % 10 == 0 then
        println(
          s"  ... completed $i / $iterations iterations, ${System.currentTimeMillis() - startTime} ms passed"
        )

      try {
        // Assertions will immediately halt the loop and throw if the math corrupts

        val res1 = VSIO.run(VSIOTestSuite.testStackSafety(10000))
        assert(res1 == 0, s"Test 1 Failed on iter $i: expected 0, got $res1")

        val res2 = VSIO.run(VSIOTestSuite.testMassiveFanOut(1000))
        assert(res2 == 1000, s"Test 2 Failed on iter $i: expected 1000, got $res2")

        val res3 = VSIO.run(VSIOTestSuite.testConcurrentFibonacci(15))
        assert(res3 == 610, s"Test 3 Failed on iter $i: expected 610, got $res3")

        val res4 = VSIO.run(VSIOTestSuite.testBlockingHandoff())
        assert(res4 == 42, s"Test 4 Failed on iter $i: expected 42, got $res4")

        val res5 = VSIO.run(VSIOTestSuite.testMassiveBlocking(50))
        assert(res5 == 50, s"Test 5 Failed on iter $i: expected 50, got $res5")

        val res6 = VSIO.run(VSIOTestSuite.testSfutexMutualExclusion(100))
        assert(res6 == 100, s"Test 6 Failed on iter $i: expected 100, got $res6")

        val res7 = VSIO.run(VSIOTestSuite.testSfutexCapacity(5, 50))
        // Permit capacity must never be breached
        assert(
          res7 <= 5,
          s"Test 7 Failed on iter $i: Capacity breach! Allowed $res7 concurrent fibers."
        )

        // The Async Boundary Check
        val res8 = VSIO.run(VSIOTestSuite.testAsyncBoundary())
        assert(res8 == 42, s"Test 8 Failed on iter $i: expected 42, got $res8")

      } catch {
        case e: Throwable =>
          println(s"\n[!] CRITICAL DISCREPANCY DETECTED ON ITERATION $i")
          e.printStackTrace()
          failed = true
      }
    }

    if !failed then {
      val totalTime = System.currentTimeMillis() - startTime
      println(s"\n--- All $iterations Statistical Tests Passed in ${totalTime}ms ---")
    }
  }
}
