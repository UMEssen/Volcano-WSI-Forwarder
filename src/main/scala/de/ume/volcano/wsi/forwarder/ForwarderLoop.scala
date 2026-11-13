package de.ume.volcano.wsi.forwarder

import java.time.Duration
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters.*
import scala.util.boundary
import scala.util.boundary.break

/** The consumer loop. One slide at a time per partition, in order, at least once:
  *
  * - success: the bundle with the PACS endpoint goes to the sink topic, then the offset is
  *   committed;
  * - transient failure: the partition is paused and the record re-read after an exponential
  *   backoff (`retryBase` … `retryMax`); the first attempt writes a non-final failure record;
  * - permanent failure, or a transient one older than `maxWait`: a final failure record, and the
  *   **original** bundle goes to the sink unchanged (SMB endpoint only), so the FHIR resource is
  *   never held back for longer than `maxWait`. A later replay adds the PACS endpoint.
  *
  * Pausing (rather than sleeping) keeps the consumer polling, so a slide that waits does not cost
  * the group membership and the other partitions keep flowing.
  */
final class ForwarderLoop(
    consumer: Consumer[String, String],
    producer: Producer[String, String],
    sinkTopic: String,
    errorsTopic: String,
    failures: FailureRecords,
    timing: Timing,
    process: StudyRecord => Either[Failure, String],
    now: () => Long = () => System.currentTimeMillis()
):
  private val log = LoggerFactory.getLogger(classOf[ForwarderLoop])
  private final case class Pending(offset: Long, attempt: Int, firstFailure: Long, resumeAt: Long)
  private val pending = scala.collection.mutable.Map.empty[TopicPartition, Pending]
  @volatile var lastPoll: Long = now()

  private enum Step:
    case Done, Retry

  def runOnce(timeout: Duration): Unit =
    val t = now()
    val due = pending.collect { case (tp, p) if p.resumeAt <= t => tp }.toSet & consumer.paused().asScala.toSet
    if due.nonEmpty then consumer.resume(due.asJava)
    val records = consumer.poll(timeout)
    lastPoll = now()
    records.partitions().asScala.foreach { tp =>
      boundary {
        records.records(tp).asScala.foreach { rec =>
          handle(tp, rec) match
            case Step.Done =>
              consumer.commitSync(Map(tp -> OffsetAndMetadata(rec.offset + 1)).asJava)
              pending.remove(tp)
            case Step.Retry =>
              consumer.seek(tp, rec.offset)
              consumer.pause(Set(tp).asJava)
              break()
        }
      }
    }

  private def delay(attempt: Int): Long =
    math.min(timing.retryMaxMs, timing.retryBaseMs * (1L << math.min(attempt - 1, 30)))

  private def handle(tp: TopicPartition, rec: ConsumerRecord[String, String]): Step =
    val origin = Origin(rec.topic, rec.partition, rec.offset, Some(rec.timestamp).filter(_ >= 0))
    StudyRecord.parse(rec.value) match
      case Left(err) =>
        log.warn("record {} is not a WSI bundle ({}); skipped", origin.key, err)
        producer.send(failures.record(errorsTopic, origin, Failure(Reason.BadRecord, err), 1, isFinal = true)).get()
        Step.Done
      case Right(study) =>
        val result =
          try process(study)
          catch case e: Exception => Left(Failure(Reason.StowFailed, s"unexpected: ${e.getClass.getSimpleName}"))
        result match
          case Right(out) =>
            producer.send(ProducerRecord(sinkTopic, rec.key, out)).get()
            Step.Done
          case Left(f) =>
            val prev = pending.get(tp).filter(_.offset == rec.offset)
            val attempt = prev.map(_.attempt + 1).getOrElse(1)
            val first = prev.map(_.firstFailure).getOrElse(now())
            if f.reason.transient && now() - first < timing.maxWaitMs then
              if attempt == 1 then producer.send(failures.record(errorsTopic, origin, f, attempt, isFinal = false)).get()
              val wait = delay(attempt)
              log.info("record {} not stored yet ({}: {}); retry {} in {} s", origin.key, f.reason.label, f.detail, attempt, wait / 1000)
              pending(tp) = Pending(rec.offset, attempt, first, now() + wait)
              Step.Retry
            else
              log.warn("record {} given up ({}: {}) after {} attempt(s); forwarded without the PACS endpoint",
                origin.key, f.reason.label, f.detail, attempt)
              producer.send(failures.record(errorsTopic, origin, f, attempt, isFinal = true)).get()
              producer.send(ProducerRecord(sinkTopic, rec.key, rec.value)).get()
              Step.Done
