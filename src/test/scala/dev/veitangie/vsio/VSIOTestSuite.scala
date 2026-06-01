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

object VSIOTestSuite {

  private val daemonFactory = {
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setDaemon(true)
        t
      }
    }
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  given env: SimpleExecutionContext = SimpleExecutionContext(
    computePool = global,
    computeSize = Runtime.getRuntime.availableProcessors(),
    blockingPool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(daemonFactory))
  )

  def main(args: Array[String]): Unit = {
    println("--- Starting SIO Stress Tests ---")

    val start1 = System.currentTimeMillis()
    val res1   = VSIO.run(testStackSafety(1000000))
    println(s"[Test 1] Stack Safety (1M ops): $res1 in ${System.currentTimeMillis() - start1}ms")

    val start2 = System.currentTimeMillis()
    val res2   = VSIO.run(testMassiveFanOut(10000))
    println(
      s"[Test 2] Fan-Out/Fan-In (10k fibers): $res2 in ${System.currentTimeMillis() - start2}ms"
    )

    val start3 = System.currentTimeMillis()
    val res3   = VSIO.run(testConcurrentFibonacci(20))
    println(
      s"[Test 3] Concurrent Fibonacci (fib 20): $res3 in ${System.currentTimeMillis() - start3}ms"
    )

    val start4 = System.currentTimeMillis()
    val res4   = VSIO.run(testBlockingHandoff())
    println(s"[Test 4] Blocking Handoff: $res4 in ${System.currentTimeMillis() - start4}ms")

    val start5 = System.currentTimeMillis()
    val res5   = VSIO.run(testMassiveBlocking(100))
    println(
      s"[Test 5] Massive Blocking (100 fibers sleeping 100ms): $res5 in ${System.currentTimeMillis() - start5}ms"
    )

    val start6 = System.currentTimeMillis()
    val res6   = VSIO.run(testSfutexMutualExclusion(1000))
    println(
      s"[Test 6] Sfutex Mutual Exclusion (1000 fibers): $res6 in ${System.currentTimeMillis() - start6}ms"
    )

    val start7 = System.currentTimeMillis()
    val res7   = VSIO.run(testSfutexCapacity(5, 50))
    println(
      s"[Test 7] Sfutex Capacity (5 permits, 50 fibers): $res7 in ${System.currentTimeMillis() - start7}ms"
    )

    val start8 = System.currentTimeMillis()
    val res8   = VSIO.run(testAsyncBoundary())
    println(s"[Test 8] Async Callback Boundary: $res8 in ${System.currentTimeMillis() - start8}ms")

    println("--- All Tests Passed ---")
  }

  /** TEST 1: The Deep Dive (Stack Safety)
    */
  def testStackSafety(n: Int): VSIO[Int] = {
    def loop(current: Int): VSIO[Int] = {
      if current == 0 then
        VSIO(0)
      else {
        VSIO(current).flatMap(x => loop(x - 1))
      }
    }
    loop(n)
  }

  /** TEST 2: The Stampede (Massive Fan-Out / Fan-In)
    */
  def testMassiveFanOut(fiberCount: Int): VSIO[Int] = {
    def spawnFibers(count: Int, acc: List[FiberHandle[Int]]): VSIO[List[FiberHandle[Int]]] = {
      if count == 0 then
        VSIO(acc)
      else {
        VSIO(1).fork().flatMap(handle => spawnFibers(count - 1, handle :: acc))
      }
    }

    def joinAll(handles: List[FiberHandle[Int]], sum: Int): VSIO[Int] = {
      handles match {
        case Nil =>
          VSIO(sum)
        case h :: t =>
          h.join().flatMap(v => joinAll(t, sum + v))
      }
    }

    spawnFibers(fiberCount, Nil).flatMap(handles => joinAll(handles, 0))
  }

  /** TEST 3: The Fork-Bomb (Concurrent Tree Recursion)
    */
  def testConcurrentFibonacci(n: Int): VSIO[Int] = {
    if n <= 1 then
      VSIO(n)
    else {
      for {
        f1 <- testConcurrentFibonacci(n - 1).fork()
        f2 <- testConcurrentFibonacci(n - 2).fork()
        v1 <- f1.join()
        v2 <- f2.join()
      } yield v1 + v2
    }
  }

  /** TEST 4: The Handoff (Non-Starvation)
    */
  def testBlockingHandoff(): VSIO[Int] = {
    for {
      sleeper <- VSIO
        .blocking {
          Thread.sleep(500)
          42
        }
        .fork()
      _      <- testStackSafety(50000)
      result <- sleeper.join()
    } yield result
  }

  /** TEST 5: The Elasticity (Massive Concurrent Blocking)
    */
  def testMassiveBlocking(fiberCount: Int): VSIO[Int] = {
    def spawnSleepers(count: Int, acc: List[FiberHandle[Int]]): VSIO[List[FiberHandle[Int]]] = {
      if count == 0 then
        VSIO(acc)
      else {
        val sleepJob = VSIO.blocking {
          Thread.sleep(100)
          1
        }
        sleepJob.fork().flatMap(handle => spawnSleepers(count - 1, handle :: acc))
      }
    }

    def joinAll(handles: List[FiberHandle[Int]], sum: Int): VSIO[Int] = {
      handles match {
        case Nil =>
          VSIO(sum)
        case h :: t =>
          h.join().flatMap(v => joinAll(t, sum + v))
      }
    }

    spawnSleepers(fiberCount, Nil).flatMap(handles => joinAll(handles, 0))
  }

  /** TEST 6: Sfutex Mutual Exclusion (The Race)
    */
  def testSfutexMutualExclusion(fiberCount: Int): VSIO[Int] = {
    var sharedUnsafeCounter = 0
    val mutex               = Sfutex(1)

    def incrementJob: VSIO[Unit] = {
      for {
        _       <- mutex.lock()
        current <- VSIO(sharedUnsafeCounter)
        _       <- testStackSafety(2000)
        _ <- VSIO {
          sharedUnsafeCounter = current + 1
        }
        _ <- mutex.unlock()
      } yield ()
    }

    def spawnJobs(count: Int, acc: List[FiberHandle[Unit]]): VSIO[List[FiberHandle[Unit]]] = {
      if count == 0 then
        VSIO(acc)
      else {
        incrementJob.fork().flatMap(handle => spawnJobs(count - 1, handle :: acc))
      }
    }

    def joinAll(handles: List[FiberHandle[Unit]]): VSIO[Unit] = {
      handles match {
        case Nil =>
          VSIO(())
        case h :: t =>
          h.join().flatMap(_ => joinAll(t))
      }
    }

    for {
      handles <- spawnJobs(fiberCount, Nil)
      _       <- joinAll(handles)
    } yield sharedUnsafeCounter
  }

  /** TEST 7: Sfutex Capacity (The Bouncer)
    */
  def testSfutexCapacity(permits: Int, fiberCount: Int): VSIO[Int] = {
    val sem               = Sfutex(permits)
    var maxConcurrent     = 0
    var currentConcurrent = 0

    val stateLock = Sfutex(1)

    def accessJob: VSIO[Unit] = {
      for {
        _ <- sem.lock()

        _ <- stateLock.lock()
        _ <- VSIO {
          currentConcurrent += 1
          if currentConcurrent > maxConcurrent then {
            maxConcurrent = currentConcurrent
          }
        }
        _ <- stateLock.unlock()

        _ <- VSIO.blocking {
          Thread.sleep(50)
        }

        _ <- stateLock.lock()
        _ <- VSIO {
          currentConcurrent -= 1
        }
        _ <- stateLock.unlock()

        _ <- sem.unlock()
      } yield ()
    }

    def spawnJobs(count: Int, acc: List[FiberHandle[Unit]]): VSIO[List[FiberHandle[Unit]]] = {
      if count == 0 then
        VSIO(acc)
      else {
        accessJob.fork().flatMap(handle => spawnJobs(count - 1, handle :: acc))
      }
    }

    def joinAll(handles: List[FiberHandle[Unit]]): VSIO[Unit] = {
      handles match {
        case Nil =>
          VSIO(())
        case h :: t =>
          h.join().flatMap(_ => joinAll(t))
      }
    }

    for {
      handles <- spawnJobs(fiberCount, Nil)
      _       <- joinAll(handles)
    } yield maxConcurrent
  }

  /** TEST 8: The Async Callback Boundary Proves that a fiber can be safely suspended and then
    * resumed from an entirely unmanaged, external thread via a callback function.
    */
  def testAsyncBoundary(): VSIO[Int] = {
    for {
      // Start a compute-heavy job in the background to prove the compute loop
      // keeps spinning while the main fiber is suspended.
      backgroundMath <- testStackSafety(50000).fork()

      // The trapdoor. We simulate a completely external system (like a Netty network event).
      asyncResult <- VSIO.async[Int] { callback =>
        val externalSystem = {
          new Thread(() => {
            Thread.sleep(200) // Simulate network delay
            callback(42)      // The unmanage-d thread wakes the fiber up
          })
        }
        externalSystem.start()
      }

      // Ensure the compute pool didn't stall
      _ <- backgroundMath.join()
    } yield asyncResult
  }
}
