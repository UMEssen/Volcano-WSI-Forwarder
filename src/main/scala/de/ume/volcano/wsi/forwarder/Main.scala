package de.ume.volcano.wsi.forwarder

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters.*

object Main:
  private val log = LoggerFactory.getLogger("de.ume.volcano.wsi.forwarder.Main")

  private def kafkaProps(cfg: Config): Properties =
    val p = Properties()
    p.put("bootstrap.servers", cfg.kafkaBootstrap)
    for u <- cfg.kafkaUser; pw <- cfg.kafkaPassword do
      def esc(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")
      p.put("security.protocol", "SASL_SSL")
      p.put("sasl.mechanism", "SCRAM-SHA-512")
      p.put("sasl.jaas.config", s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="${esc(u)}" password="${esc(pw)}";""")
      cfg.kafkaCaFile.foreach { f =>
        p.put("ssl.truststore.type", "PEM")
        p.put("ssl.truststore.location", f)
      }
    p

  def main(args: Array[String]): Unit =
    val cfg = Config.fromEnv()
    val consumerProps = kafkaProps(cfg)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId)
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    // One slide is several GB through a rate limit: allow an hour between polls.
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "3600000")
    val producerProps = kafkaProps(cfg)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760")

    val consumer = KafkaConsumer[String, String](consumerProps, StringDeserializer(), StringDeserializer())
    val producer = KafkaProducer[String, String](producerProps, StringSerializer(), StringSerializer())
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).followRedirects(HttpClient.Redirect.NEVER).build()
    val tokens = ClientCredentialsTokenProvider(cfg.oidcTokenUrl, cfg.oidcClientId, cfg.oidcClientSecret, cfg.oidcScope, http)
    val files = SmbSlideFiles(cfg.smbServer, cfg.shares)
    val forwarder = Forwarder(files, FuegoPatientSource(cfg.fuegoBaseUrl, tokens, http),
      StowClient(cfg.stowUrl, http, cfg.stowTimeout), RateLimiter(cfg.maxBytesPerSecond), cfg.dicomwebBase, cfg.issuerOfPatientId)
    val loop = ForwarderLoop(consumer, producer, cfg.sinkTopic, cfg.errorsTopic, FailureRecords(cfg.release), cfg.timing, forwarder.forward)

    // Liveness: the loop polled within the poll interval (a transfer in progress counts as alive).
    val health = HttpServer.create(InetSocketAddress(cfg.healthPort), 0)
    health.createContext("/health", ex => {
      val ok = System.currentTimeMillis() - loop.lastPoll < 3_700_000L
      val body = (if ok then "ok" else "stalled").getBytes
      ex.sendResponseHeaders(if ok then 200 else 503, body.length.toLong); ex.getResponseBody.write(body); ex.close()
    })
    health.start()

    sys.addShutdownHook(consumer.wakeup())
    log.info("forwarding {} -> research PACS -> {} (release {}, shares {})", cfg.sourceTopic, cfg.sinkTopic, cfg.release, cfg.shares.keys.mkString(","))
    consumer.subscribe(List(cfg.sourceTopic).asJava)
    try while true do loop.runOnce(Duration.ofSeconds(1))
    catch case _: WakeupException => log.info("shutting down")
    finally
      scala.util.Try(consumer.close())
      scala.util.Try(producer.close())
      scala.util.Try(files.close())
      health.stop(0)
