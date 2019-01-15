package com.twitter.scalding

import com.twitter.algebird.Aggregator.sortedReverseTake
import org.scalatest.{Matchers, WordSpec}

class MergedPipeTest extends WordSpec with Matchers {
  case class Item(
    bucket: Int,
    score: Long
  )

  "Merged pipe" should {
    "work well with topK aggregate" in {
      val items = List(Item(1, 2), Item(2, 4))
      val inputPipe = TypedPipe.from(items) ++ TypedPipe.from(items)

      val topK = inputPipe
        .groupBy(_.bucket)
        .aggregate(sortedReverseTake[Item](2)(Ordering.by(_.score)))
        .flatMap(_._2)

      // With `dedupMerge` optimization this never finishes but should.
      // Seems related to copied `PriorityQueue` instances by `Fill` function.
      //
      // Without `dedupMerge` optimization this fails with NPE.
      // It's impossible to disable it through `Config` since
      // this particular optimization (as part of `DiamondToFlatMap`) is enabled
      // unconditionally in `CascadingBackend`.
      //
      assertResult(4)(TypedPipeChecker.inMemoryToList(topK).size)
    }
  }
}
