package de.ume.volcano.wsi.forwarder

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.net.URI
import java.net.URLEncoder
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

trait TokenProvider:
  def token(): String

/** OAuth2 client-credentials grant with `client_secret_basic`, cached until shortly before expiry. */
final class ClientCredentialsTokenProvider(tokenUrl: String, clientId: String, clientSecret: String, scope: Option[String], http: HttpClient)
    extends TokenProvider:
  private val mapper = ObjectMapper()
  private var cached: Option[(String, Instant)] = None

  def token(): String = synchronized {
    cached match
      case Some((t, exp)) if Instant.now().isBefore(exp) => t
      case _ =>
        val form = (Seq("grant_type" -> "client_credentials") ++ scope.map("scope" -> _).toSeq)
          .map((k, v) => s"${URLEncoder.encode(k, UTF_8)}=${URLEncoder.encode(v, UTF_8)}").mkString("&")
        val basic = java.util.Base64.getEncoder.encodeToString(
          s"${URLEncoder.encode(clientId, UTF_8)}:${URLEncoder.encode(clientSecret, UTF_8)}".getBytes(UTF_8))
        val req = HttpRequest.newBuilder(URI.create(tokenUrl)).timeout(Duration.ofSeconds(30))
          .header("Content-Type", "application/x-www-form-urlencoded").header("Authorization", s"Basic $basic")
          .POST(HttpRequest.BodyPublishers.ofString(form)).build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        if res.statusCode() != 200 then throw IllegalStateException(s"token endpoint answered ${res.statusCode()}")
        val n = mapper.readTree(res.body())
        val t = n.path("access_token").asText("")
        if t.isEmpty then throw IllegalStateException("token endpoint returned no access_token")
        val ttl = math.max(30L, n.path("expires_in").asLong(300) - 30)
        cached = Some((t, Instant.now().plusSeconds(ttl)))
        t
  }

/** The patient's demographics as DICOM values. */
final case class Demographics(patientName: Option[String], birthDate: Option[String], sex: Option[String])

trait PatientSource:
  /** `Left` = lookup failed or ambiguous (the record is retried); `Right(None)` = no such patient. */
  def demographics(medicoId: String): Either[String, Option[Demographics]]

/** Reads the patient from Fuego by its Medico identifier, read-only (`system/Patient.rs`). */
final class FuegoPatientSource(baseUrl: String, tokens: TokenProvider, http: HttpClient) extends PatientSource:
  private val mapper = ObjectMapper()
  private val base = baseUrl.stripSuffix("/")

  def demographics(medicoId: String): Either[String, Option[Demographics]] =
    val q = URLEncoder.encode(s"${StudyRecord.MedicoSystem}|$medicoId", UTF_8)
    val req = HttpRequest.newBuilder(URI.create(s"$base/Patient?identifier=$q&_elements=name,birthDate,gender&_count=2"))
      .timeout(Duration.ofSeconds(30)).header("Accept", "application/fhir+json")
      .header("Authorization", s"Bearer ${tokens.token()}").GET().build()
    val res =
      try http.send(req, HttpResponse.BodyHandlers.ofString())
      catch case e: Exception => return Left(s"Patient search failed: ${e.getClass.getSimpleName}")
    if res.statusCode() != 200 then return Left(s"Patient search answered HTTP ${res.statusCode()}")
    val patients = mapper.readTree(res.body()).path("entry").elements().asScala.map(_.path("resource"))
      .filter(_.path("resourceType").asText() == "Patient").toSeq
    patients match
      case Seq() => Right(None)
      case Seq(p) => Right(Some(Demographics.of(p)))
      case ps => Left(s"Medico identifier matches ${ps.size} patients")

object Demographics:
  /** FHIR Patient → DICOM: the official (else first) name as `Family^Given Given`, birth date as
    * `YYYYMMDD` (only when the full date is known), gender as M/F/O.
    */
  def of(p: JsonNode): Demographics =
    val names = p.path("name").elements().asScala.toSeq
    val name = names.find(_.path("use").asText() == "official").orElse(names.headOption).map { n =>
      val family = n.path("family").asText("").trim
      val givenNames = n.path("given").elements().asScala.map(_.asText().trim).filter(_.nonEmpty).mkString(" ")
      Seq(family, givenNames).reverse.dropWhile(_.isEmpty).reverse.mkString("^")
    }.filter(_.nonEmpty)
    val birth = Option(p.path("birthDate").asText(null)).filter(_.matches("""\d{4}-\d{2}-\d{2}""")).map(_.replace("-", ""))
    val sex = p.path("gender").asText("") match
      case "male" => Some("M")
      case "female" => Some("F")
      case "other" => Some("O")
      case _ => None
    Demographics(name, birth, sex)
