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

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

final private[vsio] class Fiber(
  var current: VSIO[?],
  val stack: mutable.Stack[Any => VSIO[Any]],
  val state: AtomicReference[FiberState],
  var immediate: Any = null
) {
  def unwindStack[A](): Option[A] = {
    if stack.nonEmpty then
      current = stack.pop()(immediate)
      None
    else {
      current = null
      Some(immediate.asInstanceOf[A])
    }
  }
}
