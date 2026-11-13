package de.ume.volcano.wsi.forwarder

import java.io.ByteArrayInputStream
import java.net.http.HttpClient
import java.time.Duration
import org.scalatest.funsuite.AnyFunSuite

/** Against a real receiver, opt-in: `RECEIVER_STOW_URL=http://host:port/dicom-web/studies sbt test`.
  * Sends synthetic instances only (invented UIDs and patient). Skipped when the variable is unset.
  */
class ReceiverInteropSpec extends AnyFunSuite:
  private val url = sys.env.get("RECEIVER_STOW_URL")

  test("the receiver accepts patched, chunked single-part STOW requests") {
    assume(url.isDefined, "set RECEIVER_STOW_URL to run against a receiver")
    val stow = StowClient(url.get, HttpClient.newHttpClient(), Duration.ofMinutes(2))
    val patch = PatientPatch(Some("Synthetic^Test"), "SYN-0001", Some("TEST"), Some("20000101"), Some("O"), Some("E0/00"))
    val run = System.currentTimeMillis()
    for (i, px) <- Seq(1 -> 4096, 2 -> 5_000_000) do
      val sop = s"2.25.${run}${i}"
      val file = Synthetic.file(sop, Some(Array.tabulate[Byte](px)(j => (j % 199).toByte)))
      val instance = Patcher.open(ByteArrayInputStream(file), patch)
      assert(stow.store(instance) == Right(()), sop)
  }
