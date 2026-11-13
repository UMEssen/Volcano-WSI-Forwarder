package de.ume.volcano.wsi.forwarder

import java.io.{ByteArrayInputStream, FilterInputStream, InputStream, SequenceInputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.US_ASCII
import java.time.Duration
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Why a store failed. `transient` = worth retrying (network, 5xx, the receiver's own storage
  * failing); otherwise the request itself was refused.
  */
final case class StowFailure(detail: String, transient: Boolean)

trait InstanceSink:
  def store(instance: PatchedInstance): Either[StowFailure, Unit]

/** STOW-RS (PS3.18 §10.5) with one `application/dicom` part per request, streamed: the body is
  * the multipart prefix, the patched header, the raw rest of the file and the closing boundary,
  * sent with chunked transfer encoding. Nothing is buffered beyond the header.
  */
final class StowClient(studiesUrl: String, http: HttpClient, requestTimeout: Duration) extends InstanceSink:
  def store(instance: PatchedInstance): Either[StowFailure, Unit] =
    val boundary = s"volcano-${UUID.randomUUID()}"
    val prefix = s"--$boundary\r\nContent-Type: application/dicom\r\n\r\n".getBytes(US_ASCII)
    val suffix = s"\r\n--$boundary--\r\n".getBytes(US_ASCII)
    val body = SequenceInputStream(Seq[InputStream](
      ByteArrayInputStream(prefix), ByteArrayInputStream(instance.head), instance.rest, ByteArrayInputStream(suffix)
    ).iterator.asJavaEnumeration)
    var used = false
    val req = HttpRequest.newBuilder(URI.create(studiesUrl))
      .version(HttpClient.Version.HTTP_1_1)
      .timeout(requestTimeout)
      .header("Content-Type", s"""multipart/related; type="application/dicom"; boundary=$boundary""")
      .header("Accept", "application/dicom+json")
      // The body is a one-shot stream; a second subscription (redirect, auth retry) must fail
      // rather than send a truncated instance.
      .POST(HttpRequest.BodyPublishers.ofInputStream { () =>
        if used then throw IllegalStateException("the instance stream cannot be sent twice")
        used = true
        body
      })
      .build()
    try
      val res = http.send(req, HttpResponse.BodyHandlers.discarding())
      res.statusCode() match
        case 200 => Right(())
        // 409: every instance failed. The receiver answers that when its own storage (S3, the
        // pending table) fails, which is transient; a malformed instance is caught before sending.
        case 409 | 408 | 429 => Left(StowFailure(s"STOW answered HTTP ${res.statusCode()}", transient = true))
        case 202 => Left(StowFailure("STOW answered HTTP 202 (instance not stored)", transient = true))
        case c if c >= 500 => Left(StowFailure(s"STOW answered HTTP $c", transient = true))
        case c => Left(StowFailure(s"STOW answered HTTP $c", transient = false))
    catch
      case e: java.io.IOException => Left(StowFailure(s"STOW failed: ${e.getClass.getSimpleName}", transient = true))
      case e: java.net.http.HttpTimeoutException => Left(StowFailure("STOW timed out", transient = true))
    finally scala.util.Try(instance.close())

/** Caps the read rate (bytes per second) of one stream; shared between streams through one
  * [[RateLimiter]], so a process stays within one budget towards the NAS.
  */
final class RateLimiter(bytesPerSecond: Long):
  private var available: Double = bytesPerSecond.toDouble
  private var last: Long = System.nanoTime()

  def acquire(n: Int): Unit = if bytesPerSecond > 0 then synchronized {
    var need = n.toDouble
    while need > 0 do
      val now = System.nanoTime()
      available = math.min(bytesPerSecond.toDouble, available + (now - last) / 1e9 * bytesPerSecond)
      last = now
      val take = math.min(need, available)
      available -= take; need -= take
      if need > 0 then Thread.sleep(math.max(1L, (need / bytesPerSecond * 1000).toLong.min(200L)))
  }

final class RateLimitedInputStream(in: InputStream, limiter: RateLimiter) extends FilterInputStream(in):
  override def read(): Int =
    val b = super.read()
    if b >= 0 then limiter.acquire(1)
    b
  override def read(b: Array[Byte], off: Int, len: Int): Int =
    val n = super.read(b, off, len)
    if n > 0 then limiter.acquire(n)
    n
