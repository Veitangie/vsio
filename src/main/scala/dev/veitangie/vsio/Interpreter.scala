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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.mutable
import scala.concurrent.ExecutionContext

final private[vsio] case class Interpreter(
  tasks: ConcurrentLinkedQueue[Fiber],
  workers: ConcurrentLinkedQueue[CountDownLatch],
  finished: AtomicBoolean,
  waitingPool: ConcurrentHashMap[Fiber, Set[Fiber]],
  blockingPool: ExecutionContext,
  quantumSize: Int
) extends Runnable {
  def run(): Unit = {
    var counter = 0
    while !finished.get() do {
      val task = tasks.poll()
      if task == null then {
        counter += 1
        if counter >= 5 then {
          counter = 0
          val l = CountDownLatch(1)
          workers.add(l)
          if !tasks.isEmpty then {
            val w = workers.poll()
            if w != null then
              w.countDown()
          }
          l.await(500L, TimeUnit.MILLISECONDS)
        }
      } else {
        counter = 0
      }

      var quantum  = quantumSize
      var continue = true
      while task != null && task.current != null && continue && task.state.get() == Running &&
        quantum > 0
      do {
        continue = doStep(task)
        quantum -= 1
      }

      if task != null && continue then
        finalizeFiber(task)
    }
  }

  private def submitTask(task: Fiber): Unit = {
    tasks.add(task)
    val worker = workers.poll()
    if worker != null then {
      worker.countDown()
    }
  }

  private def doStep(fiber: Fiber): Boolean = {
    fiber.current match {
      case p: Pure[?] =>
        fiber.immediate = p.value
        fiber.unwindStack()
        true
      case sync: Sync[?] =>
        fiber.immediate = sync.evaluate()
        fiber.unwindStack()
        true
      case FlatMap(underlying, fa) =>
        fiber.current = underlying
        fiber.stack.push(fa.asInstanceOf[Any => VSIO[Any]])
        true
      case Fork(underlying) =>
        val forked = new Fiber(underlying, mutable.Stack(), AtomicReference(Running))
        submitTask(forked)
        fiber.current = Pure(FiberHandle(forked))
        true
      case Join(fiberHandle) =>
        joinHandoff(fiber, fiberHandle)
      case _: Blocking[?] =>
        blockingHandoff(fiber)
        false
      case Acquire(sfutex) =>
        sfutexAcquire(fiber, sfutex)
      case Release(sfutex) =>
        sfutexRelease(fiber, sfutex)
      case _: Async[?] =>
        asyncHandoff(fiber)
        false
    }
  }

  private def finalizeFiber(fiber: Fiber): Unit = {
    if fiber.state.get == Running then {
      if fiber.current == null then {
        while !fiber.state.compareAndSet(Running, Completed) do {}
      } else {
        submitTask(fiber)
      }
    }

    if fiber.state.get() == Completed then {
      waitingPool
        .getOrDefault(fiber, Set())
        .foreach { t =>
          var set = t.state.compareAndSet(Suspended, Running)
          while t.state.get() == Suspended do {
            set = t.state.compareAndSet(Suspended, Running)
          }
          if set then
            submitTask(t)
        }
      waitingPool.remove(fiber)
    }
  }

  private def joinHandoff(fiber: Fiber, fiberHandle: FiberHandle[Any]) = {
    if fiberHandle.fiber.state.get() == Completed then {
      fiber.current = Pure(fiberHandle.fiber.immediate)
      true
    } else {
      var s = waitingPool.computeIfAbsent(fiberHandle.fiber, _ => Set[Fiber](fiber))
      while !s(fiber) do {
        s = waitingPool.compute(fiberHandle.fiber, (_, set) => set + fiber)
      }
      while !fiber.state.compareAndSet(Running, Suspended) do {}
      if fiberHandle.fiber.state.get() == Completed then {
        var set = fiber.state.compareAndSet(Suspended, Running)
        while fiber.state.get() == Suspended do {
          set = fiber.state.compareAndSet(Suspended, Running)
        }
        set
      } else {
        false
      }
    }
  }

  private def sfutexAcquire(fiber: Fiber, sfutex: Sfutex) = {
    while fiber.state.get() == Running do {
      fiber.state.compareAndSet(Running, Suspended)
    }
    fiber.current = Pure(())

    var set                   = false
    var state: SfutexState    = null
    var newState: SfutexState = null
    var parked                = false
    while !set do {
      state = sfutex.state.get()
      if state.holders >= sfutex.capacity then {
        newState = state.copy(queue = state.queue.appended(fiber))
      } else {
        newState = state.copy(holders = state.holders + 1)
      }
      set = sfutex.state.compareAndSet(state, newState)
      parked = state.holders >= sfutex.capacity
    }

    if !parked then {
      while fiber.state.get() == Suspended do {
        fiber.state.compareAndSet(Suspended, Running)
      }
      true
    } else {
      false
    }
  }

  private def sfutexRelease(fiber: Fiber, sfutex: Sfutex) = {
    var set                   = false
    var state: SfutexState    = null
    var newState: SfutexState = null
    var toWake: Option[Fiber] = None
    while !set do {
      state = sfutex.state.get()
      val (newHolders, newQueue) = {
        if state.queue.nonEmpty then {
          toWake = state.queue.headOption
          state.holders -> state.queue.tail
        } else {
          toWake = None
          state.holders - 1 -> state.queue
        }
      }
      newState = state.copy(holders = newHolders, queue = newQueue)
      set = sfutex.state.compareAndSet(state, newState)
    }

    toWake.foreach { toWake =>
      while toWake.state.get() == Suspended do {
        toWake.state.compareAndSet(Suspended, Running)
      }
      submitTask(toWake)
    }
    fiber.current = Pure(())
    true
  }

  private def blockingHandoff(task: Fiber): Unit = {
    task.current match {
      case blocking: Blocking[?] =>
        while task.state.get() == Running do {
          task.state.compareAndSet(Running, Suspended)
        }

        blockingPool.execute({ () =>
          task.current = Pure(blocking.evaluate())
          while task.state.get() == Suspended do {
            task.state.compareAndSet(Suspended, Running)
          }
          submitTask(task)
        })
      case _ =>
    }
  }

  private def asyncHandoff(task: Fiber): Unit = {
    task.current match {
      case Async(register) =>
        while task.state.get() == Running do {
          task.state.compareAndSet(Running, Suspended)
        }

        register { value =>
          task.current = Pure(value)
          while task.state.get() == Suspended do {
            task.state.compareAndSet(Suspended, Running)
          }
          submitTask(task)
        }
      case _ =>
    }
  }
}
