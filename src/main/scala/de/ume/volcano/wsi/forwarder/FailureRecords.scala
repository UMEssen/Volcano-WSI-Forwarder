package de.ume.volcano.wsi.forwarder

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.kafka.clients.producer.ProducerRecord

/** Coordinates of the source record a failure is about. */
final case class Origin(topic: String, partition: Int, offset: Long, timestamp: Option[Long]):
  def key: String = s"$topic-$partition-$offset"

/** Failure records on the errors tier, following Volcano's failure-record contract v1
  * (docs/failure-record-contract.md): key = source coordinates, `volcano.*` headers, provenance
  * body. Redacted by construction: no slide id, path, UID or patient value; the origin coordinates
  * lead an authorised reader to the source record.
  */
final class FailureRecords(release: String):
  private val mapper = ObjectMapper()

  def record(topic: String, origin: Origin, failure: Failure, attempt: Int, isFinal: Boolean): ProducerRecord[String, String] =
    val feed = origin.topic.split('.').takeRight(2).mkString(".")
    val body = mapper.createObjectNode()
    body.put("contract", 1)
    body.put("release", release)
    body.put("final", isFinal)
    body.put("attempt", attempt)
    body.put("reason", failure.reason.label)
    body.put("detail", failure.detail)
    val rec = ProducerRecord[String, String](topic, origin.key, mapper.writeValueAsString(body))
    (Seq(
      "volcano.contract" -> "1",
      "volcano.stage" -> "forwarder",
      "volcano.feed" -> feed,
      "volcano.origin.topic" -> origin.topic,
      "volcano.origin.partition" -> origin.partition.toString,
      "volcano.origin.offset" -> origin.offset.toString,
      "volcano.tier" -> "errors",
      "volcano.final" -> isFinal.toString,
      "volcano.attempt" -> attempt.toString,
      "volcano.reason" -> failure.reason.label,
      "volcano.error.class" -> failure.reason.label,
      "volcano.release" -> release
    ) ++ origin.timestamp.map(t => "volcano.origin.timestamp" -> t.toString)).foreach((k, v) => rec.headers().add(k, v.getBytes(UTF_8)))
    rec
