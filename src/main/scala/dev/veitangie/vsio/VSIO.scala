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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch}
import scala.collection.mutable

sealed abstract class VSIO[+R] {
  def flatMap[R1](fa: R => VSIO[R1]): VSIO[R1] = FlatMap(this, fa)
  def map[R1](f: R => R1): VSIO[R1]            = FlatMap(this, r => Pure(f(r)))
  def fork(): VSIO[FiberHandle[R]]             = Fork(this)
}

final private[vsio] case class Pure[+R](value: R) extends VSIO[R]

final private[vsio] class Sync[+R](thunk: => R) extends VSIO[R] {
  def evaluate(): R = thunk
}

final private[vsio] case class FlatMap[F, +R](underlying: VSIO[F], fa: F => VSIO[R]) extends VSIO[R]

final private[vsio] case class Fork[+R](underlying: VSIO[R]) extends VSIO[FiberHandle[R]]

final private[vsio] case class Join[+R](handle: FiberHandle[R]) extends VSIO[R]

final private[vsio] class Blocking[+R](thunk: => R) extends VSIO[R] {
  def evaluate(): R = thunk
}

final private[vsio] case class Acquire(underlying: Sfutex) extends VSIO[Unit]

final private[vsio] case class Release(underlying: Sfutex) extends VSIO[Unit]

final private[vsio] case class Async[+R](register: (R => Unit) => Unit) extends VSIO[R]

object VSIO {
  def apply[A](thunk: => A): VSIO[A] = new Sync(thunk)

  def blocking[A](thunk: => A): VSIO[A] = new Blocking[A](thunk)

  def async[A](register: (A => Unit) => Unit): VSIO[A] = Async(register)

  def run[A](sio: VSIO[A])(using sec: SimpleExecutionContext): A = {
    var maybeRes: Option[A] = None
    val masterLatch         = CountDownLatch(1)
    val task = {
      new Fiber(
        sio,
        mutable.Stack({ a =>
          Sync[Unit] {
            maybeRes = Some(a.asInstanceOf[A])
            masterLatch.countDown()
          }
        }),
        AtomicReference(Running)
      )
    }

    val tasks = ConcurrentLinkedQueue[Fiber]()
    tasks.add(task)
    val workers                                           = ConcurrentLinkedQueue[CountDownLatch]()
    val finished: AtomicBoolean                           = AtomicBoolean(false)
    val waitingPool: ConcurrentHashMap[Fiber, Set[Fiber]] = ConcurrentHashMap()
    for _ <- 0 until sec.computeSize do {
      sec
        .computePool
        .execute(Interpreter(tasks, workers, finished, waitingPool, sec.blockingPool, 1024))
    }

    masterLatch.await()
    while !finished.compareAndSet(false, true) do {}

    var next = workers.poll
    while next != null do {
      next.countDown()
      next = workers.poll()
    }

    maybeRes.get
  }
}
