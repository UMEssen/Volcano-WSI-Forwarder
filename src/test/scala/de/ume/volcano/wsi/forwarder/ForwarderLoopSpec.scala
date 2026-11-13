package de.ume.volcano.wsi.forwarder

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer}
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters.*

class ForwarderLoopSpec extends AnyFunSuite:
  private val source = "volcano.transformer.fhir.r4.wsi"
  private val tp = TopicPartition(source, 0)
  private val timing = Timing(retryBaseMs = 1000, retryMaxMs = 4000, maxWaitMs = 10_000)

  private final class Harness(outcomes: Iterator[Either[Failure, String]]):
    var clock = 0L
    val consumer = MockConsumer[String, String]("earliest")
    val producer = MockProducer[String, String](true, null, StringSerializer(), StringSerializer())
    var calls = 0
    val loop = ForwarderLoop(consumer, producer, "sink", "errors", FailureRecords("t"), timing,
      _ => { calls += 1; outcomes.next() }, () => clock)
    consumer.assign(List(tp).asJava)
    consumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
    private var log = Vector.empty[(Long, String)]
    def add(offset: Long, value: String = Fixtures.bundle()): Unit =
      log :+= (offset, value)
      consumer.addRecord(ConsumerRecord(source, 0, offset, "key", value))
    /** MockConsumer drops records once polled and does not re-fetch after `seek`; a broker does.
      * When the loop pauses (and seeks back), queue the records from the seek position again.
      */
    def step(): Unit =
      val before = calls
      loop.runOnce(Duration.ZERO)
      if calls > before && consumer.paused().contains(tp) then
        val pos = consumer.position(tp)
        log.filter(_._1 >= pos).foreach((o, v) => consumer.addRecord(ConsumerRecord(source, 0, o, "key", v)))
    def sent(topic: String) = producer.history().asScala.filter(_.topic == topic)
    def committed = Option(consumer.committed(Set(tp).asJava).get(tp)).map(_.offset)
    def header(r: org.apache.kafka.clients.producer.ProducerRecord[String, String], k: String) =
      String(r.headers().lastHeader(k).value(), UTF_8)

  test("success: the enriched bundle goes to the sink under the same key, then the offset is committed") {
    val h = Harness(Iterator(Right("enriched")))
    h.add(0); h.step()
    assert(h.sent("sink").map(r => (r.key, r.value)) == Seq(("key", "enriched")))
    assert(h.sent("errors").isEmpty && h.committed.contains(1L))
  }

  test("transient failure: paused, one non-final failure record, retried after the backoff, then stored") {
    val f = Failure(Reason.StowFailed, "STOW answered HTTP 503")
    val h = Harness(Iterator(Left(f), Left(f), Right("enriched")))
    h.add(0); h.step()
    assert(h.calls == 1 && h.consumer.paused().contains(tp) && h.committed.isEmpty)
    val err = h.sent("errors").head
    assert(h.header(err, "volcano.final") == "false" && h.header(err, "volcano.reason") == "STOW_FAILED")
    assert(h.header(err, "volcano.stage") == "forwarder" && err.key == s"$source-0-0")
    h.clock = 500; h.step(); assert(h.calls == 1, "not before the backoff")
    h.clock = 1000; h.step(); assert(h.calls == 2 && h.sent("errors").size == 1, "second attempt: no new record")
    h.clock = 3000; h.step()
    assert(h.calls == 3 && h.sent("sink").map(_.value) == Seq("enriched") && h.committed.contains(1L))
  }

  test("a transient failure older than maxWait: final record, original bundle forwarded without the PACS endpoint") {
    val f = Failure(Reason.StowFailed, "down")
    val h = Harness(Iterator.continually(Left(f)))
    h.add(0); h.step()
    h.clock = 11_000; h.step()
    val finals = h.sent("errors").filter(r => h.header(r, "volcano.final") == "true")
    assert(finals.size == 1 && h.sent("sink").map(_.value) == Seq(Fixtures.bundle()) && h.committed.contains(1L))
  }

  test("a permanent failure is final at once and forwards the original bundle") {
    val h = Harness(Iterator(Left(Failure(Reason.StowRefused, "STOW answered HTTP 413"))))
    h.add(0); h.step()
    assert(h.sent("errors").map(r => h.header(r, "volcano.final")) == Seq("true"))
    assert(h.sent("sink").map(_.value) == Seq(Fixtures.bundle()) && h.committed.contains(1L))
  }

  test("a record that is not a WSI bundle is skipped with a final failure record, nothing forwarded") {
    val h = Harness(Iterator.empty)
    h.add(0, """{"resourceType":"Patient"}"""); h.step()
    assert(h.calls == 0 && h.sent("sink").isEmpty && h.committed.contains(1L))
    assert(h.header(h.sent("errors").head, "volcano.reason") == "BAD_RECORD")
  }

  test("order holds: a waiting record blocks the ones behind it on its partition") {
    val f = Failure(Reason.PatientUnresolved, "not yet")
    val h = Harness(Iterator(Left(f), Right("first"), Right("second")))
    h.add(0); h.add(1); h.step()
    assert(h.calls == 1 && h.sent("sink").isEmpty)
    h.clock = 1000; h.step(); h.step()
    assert(h.sent("sink").map(_.value) == Seq("first", "second") && h.committed.contains(2L))
  }

  test("failure records are redacted: no detail beyond the reason text, no slide or patient values") {
    val h = Harness(Iterator(Left(Failure(Reason.StowRefused, "STOW answered HTTP 413"))))
    h.add(0); h.step()
    val body = h.sent("errors").head.value
    assert(!body.contains("4711") && !body.contains("E1/99") && !body.contains("2.25."))
  }
