# VSIO: A Grokable M:N Fiber Scheduler

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0) [![JitPack](https://jitpack.io/v/Veitangie/vsio.svg)](https://jitpack.io/#Veitangie/vsio)

`VSIO` ([V]eitangie's [S]imple [IO]) is a lock-free, purely functional cooperative lightweight thread (fiber) runtime built from scratch in Scala 3.

Production effect systems often span hundreds of files and thousands of lines of dense, optimized and tightly packed with logic code. At the time of writing this readme `VSIO` fits fully in just 7 files, which makes it infinitely easier to understand.
I really wanted to see and feel how standard I/O effect systems work under the hood, so I built one. It's lacking QoL features and even some essential capabilities to call it a production ready runtime (namely - interrupts, error handling and stack traces), but it does the job of showing the way a real runtime would work.

As is mandatory in the modern world, the runtime features:

- Monadic combinators - `pure`, `map`, `flatMap` - what more do we really need?
- Stackful userspace fibers, preventing heavy OS context switching in highly concurrent scenarios
- API for integration of OS thread level blocking calls and asynchronous runtimes like NIO
- Userspace mutex implementation for the runtime (`vsfutex` - [V]eitangie's [S]imple [futex])
- Full fork-join API for real parallelism
- Cooperative scheduling and shared task pool among threads
- Test suite that covers both functional and non-functional cases (more than 1.3 million milliseconds total stress test duration per launch)

## Installation

`VSIO` is published via JitPack. To use it in your Scala 3 project, add the JitPack resolver and the library dependency to your `build.sbt`:

```scala
resolvers += "JitPack" at "https://jitpack.io"

libraryDependencies += "com.github.veitangie" %% "vsio" % "1.0.0"
```

> Disclaimer on AI usage: An LLM (Google Gemini Pro 3.1) was used to produce the test suites, the suites left unchanged - hence the comments
