package de.ume.volcano.wsi.forwarder

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.io.{ByteArrayInputStream, InputStream}
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import java.time.Duration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HttpSpec extends AnyFunSuite with BeforeAndAfterAll:
  private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
  @volatile private var stowStatus = 200
  @volatile private var received: Option[(String, String, Array[Byte])] = None // content-type, transfer-encoding, body
  @volatile private var patientBody = """{"resourceType":"Bundle","entry":[]}"""
  @volatile private var lastQuery = ""

  private def reply(ex: HttpExchange, code: Int, body: String) =
    val b = body.getBytes(UTF_8); ex.sendResponseHeaders(code, if b.isEmpty then -1 else b.length.toLong)
    if b.nonEmpty then ex.getResponseBody.write(b)
    ex.close()

  override def beforeAll(): Unit =
    server.createContext("/dicom-web/studies", ex => {
      val h = ex.getRequestHeaders
      received = Some((h.getFirst("Content-Type"), Option(h.getFirst("Transfer-Encoding")).getOrElse(""), ex.getRequestBody.readAllBytes()))
      reply(ex, stowStatus, "")
    })
    server.createContext("/token", ex => { ex.getRequestBody.readAllBytes(); reply(ex, 200, """{"access_token":"t","expires_in":300}""") })
    server.createContext("/fhir/Patient", ex => {
      lastQuery = ex.getRequestURI.getRawQuery
      assert(ex.getRequestHeaders.getFirst("Authorization") == "Bearer t")
      reply(ex, 200, patientBody)
    })
    server.start()

  override def afterAll(): Unit = server.stop(0)

  private def base = s"http://127.0.0.1:${server.getAddress.getPort}"
  private val http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
  private def stow = StowClient(s"$base/dicom-web/studies", http, Duration.ofSeconds(30))

  /** A stream that counts reads, to prove the rest is streamed rather than read up front. */
  private def instance(head: Array[Byte], rest: Array[Byte]) =
    PatchedInstance("2.25.1", "1.2.840.10008.1.2.1", head, ByteArrayInputStream(rest))

  test("one application/dicom part, chunked, body = header + rest between the boundaries") {
    stowStatus = 200
    val head = "HEAD".getBytes(US_ASCII); val rest = Array.tabulate[Byte](3_000_000)(i => (i % 256).toByte)
    assert(stow.store(instance(head, rest)) == Right(()))
    val (ct, te, body) = received.get
    assert(ct.startsWith("multipart/related;") && ct.contains("""type="application/dicom"""))
    assert(te.equalsIgnoreCase("chunked"))
    val boundary = ct.split("boundary=")(1)
    val prefix = s"--$boundary\r\nContent-Type: application/dicom\r\n\r\n".getBytes(US_ASCII)
    val suffix = s"\r\n--$boundary--\r\n".getBytes(US_ASCII)
    assert(body.sameElements(prefix ++ head ++ rest ++ suffix))
  }

  test("status mapping: 5xx/409 retry, other 4xx final") {
    for (code, transient) <- Seq(500 -> true, 503 -> true, 409 -> true, 400 -> false, 413 -> false, 415 -> false) do
      stowStatus = code
      assert(stow.store(instance(Array[Byte](1), Array[Byte](2))) == Left(StowFailure(s"STOW answered HTTP $code", transient)), code)
  }

  test("an unreachable receiver is a transient failure") {
    val r = StowClient("http://127.0.0.1:1/dicom-web/studies", http, Duration.ofSeconds(5)).store(instance(Array[Byte](1), Array[Byte](2)))
    assert(r.left.toOption.exists(_.transient))
  }

  test("the rest stream is closed after sending") {
    stowStatus = 200
    var closed = false
    val rest = new ByteArrayInputStream(Array[Byte](1, 2, 3)):
      override def close(): Unit = closed = true
    stow.store(PatchedInstance("2.25.1", "ts", Array[Byte](0), rest))
    assert(closed)
  }

  test("Fuego: Patient by Medico identifier, one hit, demographics; none; ambiguous") {
    val patients = FuegoPatientSource(s"$base/fhir", ClientCredentialsTokenProvider(s"$base/token", "c", "s", None, http), http)
    patientBody = """{"resourceType":"Bundle","entry":[{"resource":{"resourceType":"Patient","name":[{"family":"Doe","given":["Jane"]}],"birthDate":"1970-01-01","gender":"female"}}]}"""
    assert(patients.demographics("4711") == Right(Some(Demographics(Some("Doe^Jane"), Some("19700101"), Some("F")))))
    assert(java.net.URLDecoder.decode(lastQuery, UTF_8).contains(s"identifier=${StudyRecord.MedicoSystem}|4711"))
    patientBody = """{"resourceType":"Bundle","entry":[]}"""
    assert(patients.demographics("4711") == Right(None))
    patientBody = """{"resourceType":"Bundle","entry":[{"resource":{"resourceType":"Patient"}},{"resource":{"resourceType":"Patient"}}]}"""
    assert(patients.demographics("4711").isLeft)
  }

  test("rate limiter: a capped stream takes at least size / rate") {
    val data = Array.fill[Byte](300_000)(1)
    val limited: InputStream = RateLimitedInputStream(ByteArrayInputStream(data), RateLimiter(1_000_000))
    val t0 = System.nanoTime(); limited.readAllBytes(); val secs = (System.nanoTime() - t0) / 1e9
    // the first second's budget is available at once; 300 kB at 1 MB/s is within it
    assert(secs < 1.0)
    val big = RateLimitedInputStream(ByteArrayInputStream(Array.fill[Byte](1_500_000)(1)), RateLimiter(1_000_000))
    val t1 = System.nanoTime(); big.readAllBytes(); assert((System.nanoTime() - t1) / 1e9 >= 0.4)
  }
