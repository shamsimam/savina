Savina
======

Savina is an Actor Benchmark Suite.

## Brief Description

Our goal is to provide a standard benchmark suite that enables researchers and application developers to compare different actor implementations and identify those that deliver the best performance for a given use-case.
The benchmarks in Savina are diverse, realistic, and represent compute (rather than I/O) intensive applications.
They range from popular micro-benchmarks to classical concurrency problems to applications that demonstrate various styles of parallelism.
Implementations of the benchmarks on various actor libraries will be made publicly available through this open source release.
This will allow other developers and researchers to compare the performance of their actor libraries on these common set of benchmarks.

Please refer to the following paper for further details: <br />
<a href="http://soft.vub.ac.be/AGERE14/papers/ageresplash2014_submission_19.pdf">Savina - An Actor Benchmark Suite</a>.
<a href="mailto:shams@rice.edu">Shams Imam</a>,
<a href="mailto:vsarkar@rice.edu">Vivek Sarkar</a>.
4th International Workshop on Programming based on Actors, Agents, and Decentralized Control (<a href="http://soft.vub.ac.be/AGERE14/">AGERE! 2014</a>),
October 2014.

## Supported actor libraries

* <a href="http://akka.io/">Akka</a> 2.3.2
* <a href="http://code.google.com/p/functionaljava/">Functional-Java</a> 4.1
* <a href="http://gpars.codehaus.org/">GPars</a> 1.2.1
* <a href="http://wiki.rice.edu/confluence/display/PARPROG/HJ+Library">Habanero-Java library</a> 0.1.3 (includes actors and selectors)
* <a href="http://code.google.com/p/jetlang/">Jetlang</a> 0.2.12
* <a href="http://jumi.fi/actors.html">Jumi</a> 0.1.196
* <a href="http://liftweb.net/">Lift</a> 2.6-M4
* <a href="http://docs.scala-lang.org/overviews/core/actors.html">Scala</a> 2.11.0
* <a href="http://github.com/scalaz/scalaz">Scalaz</a> 7.1.0-M6.

