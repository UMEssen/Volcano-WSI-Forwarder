package de.ume.volcano.wsi.forwarder

import java.time.Duration

final case class Timing(retryBaseMs: Long, retryMaxMs: Long, maxWaitMs: Long)

final case class Config(
    sourceTopic: String,
    sinkTopic: String,
    errorsTopic: String,
    groupId: String,
    kafkaBootstrap: String,
    kafkaUser: Option[String],
    kafkaPassword: Option[String],
    kafkaCaFile: Option[String],
    smbServer: String,
    shares: Map[String, ShareLogin],
    fuegoBaseUrl: String,
    oidcTokenUrl: String,
    oidcClientId: String,
    oidcClientSecret: String,
    oidcScope: Option[String],
    stowUrl: String,
    stowTimeout: Duration,
    dicomwebBase: String,
    issuerOfPatientId: Option[String],
    maxBytesPerSecond: Long,
    healthPort: Int,
    release: String,
    timing: Timing
)

object Config:
  /** `SMB_SHARES=digitalpathology,dxpatho` with `SMB_<SHARE>_USER`, `SMB_<SHARE>_PASSWORD` and
    * optionally `SMB_<SHARE>_DOMAIN` per share (share name upper-cased, non-alphanumerics as `_`).
    */
  def shareVar(share: String, suffix: String): String =
    s"SMB_${share.toUpperCase.replaceAll("[^A-Z0-9]", "_")}_$suffix"

  def fromEnv(env: Map[String, String] = sys.env): Config =
    def opt(k: String) = env.get(k).map(_.trim).filter(_.nonEmpty)
    def req(k: String) = opt(k).getOrElse(throw IllegalArgumentException(s"$k is required"))
    def secs(k: String, d: Long) = opt(k).map(_.toLong).getOrElse(d) * 1000L
    val shares = req("SMB_SHARES").split(',').map(_.trim).filter(_.nonEmpty).map { s =>
      s -> ShareLogin(req(shareVar(s, "USER")), req(shareVar(s, "PASSWORD")), opt(shareVar(s, "DOMAIN")).getOrElse(""))
    }.toMap
    val sink = opt("SINK_TOPIC").getOrElse("volcano.forwarder.fhir.r4.wsi")
    Config(
      sourceTopic = opt("SOURCE_TOPIC").getOrElse("volcano.transformer.fhir.r4.wsi"),
      sinkTopic = sink,
      errorsTopic = opt("ERRORS_TOPIC").getOrElse(s"$sink.errors"),
      groupId = opt("KAFKA_GROUP_ID").getOrElse("volcano-forwarder-wsi"),
      kafkaBootstrap = req("KAFKA_BOOTSTRAP_SERVERS"),
      kafkaUser = opt("KAFKA_SASL_USERNAME"),
      kafkaPassword = opt("KAFKA_SASL_PASSWORD"),
      kafkaCaFile = opt("KAFKA_SSL_CA_FILE"),
      smbServer = req("SMB_SERVER"),
      shares = shares,
      fuegoBaseUrl = req("FUEGO_BASE_URL"),
      oidcTokenUrl = req("OIDC_TOKEN_URL"),
      oidcClientId = req("OIDC_CLIENT_ID"),
      oidcClientSecret = req("OIDC_CLIENT_SECRET"),
      oidcScope = opt("OIDC_SCOPE"),
      stowUrl = req("STOW_URL"),
      stowTimeout = Duration.ofMillis(secs("STOW_TIMEOUT_SECONDS", 3600)),
      dicomwebBase = req("DICOMWEB_BASE_URL"),
      issuerOfPatientId = opt("ISSUER_OF_PATIENT_ID"),
      maxBytesPerSecond = opt("MAX_BYTES_PER_SECOND").map(_.toLong).getOrElse(0L),
      healthPort = opt("HEALTH_PORT").map(_.toInt).getOrElse(8080),
      release = opt("VOLCANO_RELEASE").getOrElse("dev"),
      timing = Timing(
        retryBaseMs = secs("RETRY_BASE_SECONDS", 60),
        retryMaxMs = secs("RETRY_MAX_SECONDS", 3600),
        maxWaitMs = secs("MAX_WAIT_SECONDS", 7 * 24 * 3600)
      )
    )
