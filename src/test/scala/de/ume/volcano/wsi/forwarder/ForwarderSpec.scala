package de.ume.volcano.wsi.forwarder

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{ByteArrayInputStream, SequenceInputStream}
import org.dcm4che3.data.Tag
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters.*

class ForwarderSpec extends AnyFunSuite:
  import Fixtures.*

  private val jane = Demographics(Some("Doe^Jane"), Some("19700101"), Some("F"))

  final class FakePatients(answer: Either[String, Option[Demographics]]) extends PatientSource:
    var asked = Seq.empty[String]
    def demographics(medicoId: String) = { asked :+= medicoId; answer }

  /** Collects what would have been sent, as whole files. */
  final class CollectingSink(fail: Option[StowFailure] = None) extends InstanceSink:
    var stored = Map.empty[String, Array[Byte]]
    def store(i: PatchedInstance) =
      val bytes = SequenceInputStream(ByteArrayInputStream(i.head), i.rest).readAllBytes()
      fail.toLeft { stored += i.sopInstanceUid -> bytes }

  private def forwarder(files: SlideFiles, patients: PatientSource, sink: InstanceSink) =
    Forwarder(files, patients, sink, RateLimiter(0), "https://pacs.example/dicom-web", Some("UME"))

  private def record(json: String = bundle()) = StudyRecord.parse(json).toOption.get

  test("every instance is stored with the patient patched in, and the bundle gains the PACS endpoint") {
    val sink = CollectingSink(); val patients = FakePatients(Right(Some(jane)))
    val out = forwarder(slide(), patients, sink).forward(record())
    assert(patients.asked == Seq("4711"))
    assert(sink.stored.keySet == Sops.toSet)
    sink.stored.values.foreach { bytes =>
      val (_, ds) = Synthetic.read(bytes)
      assert(ds.getString(Tag.PatientName) == "Doe^Jane" && ds.getString(Tag.PatientID) == "4711")
      assert(ds.getString(Tag.IssuerOfPatientID) == "UME" && ds.getString(Tag.AccessionNumber) == "E1/99")
    }
    val study = ObjectMapper().readTree(out.toOption.get).path("entry").get(0).path("resource")
    val contained = study.path("contained").elements().asScala.toSeq
    assert(contained.map(_.path("id").asText()) == Seq("smb", "pacs"), "the SMB endpoint stays, as provenance")
    val pacs = contained(1)
    assert(pacs.path("connectionType").path("code").asText() == "dicom-wado-rs")
    assert(pacs.path("address").asText() == "https://pacs.example/dicom-web")
    assert(study.path("endpoint").elements().asScala.map(_.path("reference").asText()).toSeq == Seq("#smb", "#pacs"))
  }

  test("adding the endpoint twice keeps one") {
    val r = record()
    val once = StudyRecord.withPacsEndpoint(r, "https://pacs.example/dicom-web")
    val twice = StudyRecord.withPacsEndpoint(StudyRecord.parse(once).toOption.get, "https://pacs.example/dicom-web")
    val study = ObjectMapper().readTree(twice).path("entry").get(0).path("resource")
    assert(study.path("contained").size == 2 && study.path("endpoint").size == 2)
  }

  test("no Medico identifier on the subject is permanent; nothing is read or sent") {
    val files = slide(); val sink = CollectingSink()
    val r = forwarder(files, FakePatients(Right(Some(jane))), sink).forward(record(bundle(medico = None)))
    assert(r.left.toOption.map(_.reason).contains(Reason.NoSubject))
    assert(files.opened == 0 && sink.stored.isEmpty)
  }

  test("patient lookup: failure and unknown patient are retried, nothing is sent") {
    for (answer, reason) <- Seq(Left("HTTP 503") -> Reason.PatientLookupFailed, Right(None) -> Reason.PatientUnresolved) do
      val sink = CollectingSink()
      val r = forwarder(slide(), FakePatients(answer), sink).forward(record())
      assert(r.left.toOption.map(_.reason).contains(reason) && reason.transient && sink.stored.isEmpty)
  }

  test("an instance missing from the DICOMDIR is retried before anything is sent") {
    val sink = CollectingSink()
    val r = forwarder(slide(), FakePatients(Right(Some(jane))), sink).forward(record(bundle(sops = Sops :+ "2.25.13")))
    assert(r.left.toOption.map(_.reason).contains(Reason.InstanceMissing) && sink.stored.isEmpty)
  }

  test("a STOW failure maps to STOW_FAILED (retry) or STOW_REFUSED (final)") {
    for (f, reason) <- Seq(StowFailure("503", transient = true) -> Reason.StowFailed, StowFailure("413", transient = false) -> Reason.StowRefused) do
      val r = forwarder(slide(), FakePatients(Right(Some(jane))), CollectingSink(Some(f))).forward(record())
      assert(r.left.toOption.map(_.reason).contains(reason))
  }

  test("a file whose SOP differs from the DICOMDIR is refused") {
    val files = slide()
    val swapped = MemoryFiles(files.files.map { case (k @ (_, p), v) =>
      if p.endsWith(s"${Sops.head}.dcm") then k -> Synthetic.file("2.25.99", Some(Array.fill[Byte](10)(0))) else k -> v
    })
    val r = forwarder(swapped, FakePatients(Right(Some(jane))), CollectingSink()).forward(record())
    assert(r.left.toOption.map(_.reason).contains(Reason.InvalidDicom))
  }

  test("unreadable or foreign bundles are rejected by the parser") {
    assert(StudyRecord.parse("{").isLeft)
    assert(StudyRecord.parse("""{"resourceType":"Patient"}""").isLeft)
    assert(StudyRecord.parse(bundle(sops = Seq())).isLeft)
    val r = record()
    assert(r.studyInstanceUid == StudyUid && r.smb == SmbFolder("nas.example", "share-a", "2026-01-01/E1_99-1A.1_1"))
    assert(r.medicoId.contains("4711") && r.caseNumber.contains("E1/99"))
  }

  test("demographics: official name first, partial dates dropped, gender mapped") {
    val m = ObjectMapper()
    val d = Demographics.of(m.readTree("""{"name":[{"use":"usual","family":"X"},{"use":"official","family":"Doe","given":["Jane","Ann"]}],"birthDate":"1970-01-01","gender":"female"}"""))
    assert(d == Demographics(Some("Doe^Jane Ann"), Some("19700101"), Some("F")))
    assert(Demographics.of(m.readTree("""{"name":[{"family":"Doe"}],"birthDate":"1970","gender":"unknown"}""")) == Demographics(Some("Doe"), None, None))
    assert(Demographics.of(m.readTree("""{"name":[{"given":["Jane"]}]}""")).patientName.contains("^Jane"))
  }
