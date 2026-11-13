package de.ume.volcano.wsi.forwarder

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import java.net.URI
import scala.jdk.CollectionConverters.*
import scala.util.boundary
import scala.util.boundary.break

/** Where a slide's files are: `smb://<host>/<share>/<folder>/`. */
final case class SmbFolder(host: String, share: String, folder: String)

/** What the forwarder needs from one transformer bundle (an all-PUT `batch` with one
  * `ImagingStudy`, as the Volcano WSI transformer writes it).
  */
final case class StudyRecord(
    bundle: ObjectNode,
    study: ObjectNode,
    studyInstanceUid: String,
    sopInstanceUids: Seq[String],
    smb: SmbFolder,
    medicoId: Option[String],
    caseNumber: Option[String]
)

object StudyRecord:
  val MedicoSystem = "https://uk-essen.de/HIS/Cerner/Medico"
  val CaseSystem = "https://uk-essen.de/fhir/isp/fall"
  val PacsEndpointId = "pacs"
  private val mapper = ObjectMapper()

  private def text(n: JsonNode, field: String): Option[String] =
    Option(n.get(field)).filter(_.isTextual).map(_.asText().trim).filter(_.nonEmpty)
  private def arr(n: JsonNode, field: String): Seq[JsonNode] =
    Option(n.get(field)).filter(_.isArray).map(_.elements().asScala.toSeq).getOrElse(Seq.empty)

  def parse(json: String): Either[String, StudyRecord] =
    try boundary {
      val bundle = mapper.readTree(json) match
        case o: ObjectNode if text(o, "resourceType").contains("Bundle") => o
        case _ => break(Left("not a FHIR Bundle"))
      val studies = arr(bundle, "entry").flatMap(e => Option(e.get("resource"))).collect {
        case o: ObjectNode if text(o, "resourceType").contains("ImagingStudy") => o
      }
      val study = studies match
        case Seq(s) => s
        case s => break(Left(s"expected one ImagingStudy, found ${s.size}"))
      val uid = arr(study, "identifier").find(i => text(i, "system").contains("urn:dicom:uid")).flatMap(text(_, "value"))
        .map(_.stripPrefix("urn:oid:")).getOrElse(break(Left("no urn:dicom:uid identifier")))
      val sops = arr(study, "series").flatMap(s => arr(s, "instance")).flatMap(text(_, "uid"))
      if sops.isEmpty then break(Left("no instances"))
      val smbAddress = arr(study, "contained")
        .find(c => text(c, "resourceType").contains("Endpoint") && text(c, "address").exists(_.startsWith("smb://")))
        .flatMap(text(_, "address")).getOrElse(break(Left("no SMB endpoint")))
      val u = URI.create(smbAddress)
      val parts = u.getPath.split('/').filter(_.nonEmpty)
      if parts.length < 2 then break(Left("SMB endpoint has no share and folder"))
      val medico = Option(study.get("subject")).flatMap(s => Option(s.get("identifier")))
        .filter(i => text(i, "system").contains(MedicoSystem)).flatMap(text(_, "value"))
      val caseNo = arr(study, "basedOn").flatMap(r => Option(r.get("identifier")))
        .find(i => text(i, "system").contains(CaseSystem)).flatMap(text(_, "value"))
      Right(StudyRecord(bundle, study, uid, sops, SmbFolder(u.getHost, parts.head, parts.tail.mkString("/")), medico, caseNo))
    }
    catch case e: Exception => Left(s"unreadable bundle: ${e.getClass.getSimpleName}")

  /** The bundle with a second contained Endpoint (DICOMweb at the research PACS) beside the SMB
    * one, which stays as the provenance of the files. Idempotent: an existing `pacs` endpoint is
    * replaced.
    */
  def withPacsEndpoint(r: StudyRecord, dicomwebBase: String): String =
    val bundle = r.bundle.deepCopy()
    val study = bundle.get("entry").elements().asScala.map(_.get("resource"))
      .collectFirst { case o: ObjectNode if text(o, "resourceType").contains("ImagingStudy") => o }.get
    val contained = Option(study.get("contained")).collect { case a: ArrayNode => a }.getOrElse(study.putArray("contained"))
    (contained.size - 1 to 0 by -1).foreach(i => if text(contained.get(i), "id").contains(PacsEndpointId) then contained.remove(i))
    val ep = contained.addObject()
    ep.put("resourceType", "Endpoint")
    ep.put("id", PacsEndpointId)
    ep.put("status", "active")
    val ct = ep.putObject("connectionType")
    ct.put("system", "http://terminology.hl7.org/CodeSystem/endpoint-connection-type")
    ct.put("code", "dicom-wado-rs")
    ct.put("display", "DICOM WADO-RS")
    ep.put("name", "Research PACS")
    ep.putArray("payloadType").addObject().put("text", "DICOM WADO-RS")
    ep.putArray("payloadMimeType").add("application/dicom")
    ep.put("address", dicomwebBase)
    val refs = Option(study.get("endpoint")).collect { case a: ArrayNode => a }.getOrElse(study.putArray("endpoint"))
    if !refs.elements().asScala.exists(n => text(n, "reference").contains(s"#$PacsEndpointId")) then
      refs.addObject().put("reference", s"#$PacsEndpointId")
    mapper.writeValueAsString(bundle)
