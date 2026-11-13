package de.ume.volcano.wsi.forwarder

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import org.dcm4che3.data.{Attributes, Sequence, Tag, UID, VR}
import org.dcm4che3.io.DicomOutputStream

/** Synthetic data only: invented UIDs under 2.25 and an invented patient. */
object Fixtures:
  val StudyUid = "2.25.1001"
  val Sops = Seq("2.25.11", "2.25.12")

  /** The shape the Volcano WSI transformer writes (trimmed to what the forwarder reads). */
  def bundle(medico: Option[String] = Some("4711"), sops: Seq[String] = Sops, caseNo: Option[String] = Some("E1/99")): String =
    val subject = medico.map(m => s""""subject":{"type":"Patient","identifier":{"system":"${StudyRecord.MedicoSystem}","value":"$m"}},""").getOrElse("")
    val basedOn = caseNo.map(c => s""""basedOn":[{"type":"ServiceRequest","identifier":{"system":"${StudyRecord.CaseSystem}","value":"$c"}}],""").getOrElse("")
    val instances = sops.zipWithIndex.map((s, i) => s"""{"uid":"$s","sopClass":{"system":"urn:ietf:rfc:3986","code":"urn:oid:${Synthetic.Wsi}"},"number":${i + 1}}""").mkString(",")
    s"""{"resourceType":"Bundle","type":"batch","entry":[{"fullUrl":"urn:uuid:abc","resource":{
       |"resourceType":"ImagingStudy","id":"abc",
       |"contained":[{"resourceType":"Endpoint","id":"smb","status":"active","address":"smb://nas.example/share-a/2026-01-01/E1_99-1A.1_1/"}],
       |"identifier":[{"system":"urn:dicom:uid","value":"urn:oid:$StudyUid"}],
       |"status":"available",$subject$basedOn
       |"endpoint":[{"reference":"#smb"}],
       |"series":[{"uid":"2.25.1002","instance":[$instances]}]},
       |"request":{"method":"PUT","url":"ImagingStudy/abc"}}]}""".stripMargin

  def dicomDir(files: Seq[(String, String)]): Array[Byte] =
    val ds = Attributes()
    val seq: Sequence = ds.newSequence(Tag.DirectoryRecordSequence, files.size + 1)
    val study = Attributes(); study.setString(Tag.DirectoryRecordType, VR.CS, "STUDY"); study.setString(Tag.StudyInstanceUID, VR.UI, StudyUid); seq.add(study)
    files.foreach { (sop, file) =>
      val r = Attributes()
      r.setString(Tag.DirectoryRecordType, VR.CS, "IMAGE")
      r.setString(Tag.ReferencedFileID, VR.CS, file)
      r.setString(Tag.ReferencedSOPInstanceUIDInFile, VR.UI, sop)
      r.setString(Tag.ReferencedSOPClassUIDInFile, VR.UI, Synthetic.Wsi)
      seq.add(r)
    }
    val out = ByteArrayOutputStream()
    val dos = DicomOutputStream(out, UID.ExplicitVRLittleEndian)
    dos.writeDataset(Attributes.createFileMetaInformation("2.25.900", UID.MediaStorageDirectoryStorage, UID.ExplicitVRLittleEndian), ds)
    dos.close()
    out.toByteArray

  /** A slide folder in memory: share → path → bytes. */
  final class MemoryFiles(val files: Map[(String, String), Array[Byte]]) extends SlideFiles:
    var opened = 0
    def open(share: String, path: String): InputStream =
      opened += 1
      ByteArrayInputStream(files.getOrElse((share, path), throw java.io.FileNotFoundException(path)))
    def close(): Unit = ()

  def slide(pixels: Int = 50_000): MemoryFiles =
    val folder = "2026-01-01/E1_99-1A.1_1"
    val dcm = Sops.map(s => s -> Synthetic.file(s, Some(Array.tabulate[Byte](pixels)(i => (i % 253).toByte))))
    MemoryFiles(
      Map(("share-a", s"$folder/DICOMDIR") -> dicomDir(Sops.map(s => s -> s"$s.dcm"))) ++
        dcm.map((s, b) => ("share-a", s"$folder/$s.dcm") -> b)
    )
