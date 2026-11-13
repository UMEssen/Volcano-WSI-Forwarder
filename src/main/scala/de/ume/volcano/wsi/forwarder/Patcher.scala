package de.ume.volcano.wsi.forwarder

import java.io.{ByteArrayOutputStream, InputStream}
import org.dcm4che3.data.{Attributes, Tag, UID, VR}
import org.dcm4che3.io.{DicomInputStream, DicomOutputStream}

/** The patient attributes written into every instance of a slide. `None` leaves an attribute
  * empty (present, zero length), the way the scanner writes them.
  */
final case class PatientPatch(
    patientName: Option[String],
    patientId: String,
    issuerOfPatientId: Option[String],
    birthDate: Option[String],
    sex: Option[String],
    accessionNumber: Option[String]
)

/** One instance, ready to stream: the rewritten header (preamble, File Meta, data set up to and
  * including the pixel data element's header) and the rest of the original file, untouched.
  */
final case class PatchedInstance(sopInstanceUid: String, transferSyntax: String, head: Array[Byte], rest: InputStream)
    extends AutoCloseable:
  def close(): Unit = rest.close()

final class PatchError(msg: String) extends Exception(msg)

/** Rewrites the patient module of a DICOM file without reading its pixel data into memory.
  *
  * dcm4che reads the File Meta and the data set up to the pixel data element and stops right after
  * that element's header; the stream then sits at the first byte of its value. The header is
  * re-encoded with the patch applied, the pixel data element's header is written as read, and
  * everything after it is passed through as raw bytes. Only header attributes change; the image
  * data and every UID stay byte-identical.
  */
object Patcher:
  /** Deflated data sets cannot be split into a re-encoded header and a raw remainder. */
  private val Unsupported = Set(
    "1.2.840.10008.1.2.1.99", // Deflated Explicit VR Little Endian
    "1.2.840.10008.1.2.4.95", // JPIP Referenced Deflate
    "1.2.840.10008.1.2.4.205" // JPIP HTJ2K Referenced Deflate
  )

  def open(in: InputStream, patch: PatientPatch): PatchedInstance =
    val dis = DicomInputStream(in)
    try
      val fmi = Option(dis.readFileMetaInformation()).getOrElse(throw PatchError("no File Meta Information (not a PS3.10 file)"))
      val ts = Option(fmi.getString(Tag.TransferSyntaxUID)).getOrElse(throw PatchError("no TransferSyntaxUID in the File Meta"))
      if Unsupported.contains(ts) then throw PatchError(s"transfer syntax $ts cannot be streamed")
      val ds = dis.readDatasetUntilPixelData()
      val sop = Option(ds.getString(Tag.SOPInstanceUID)).orElse(Option(fmi.getString(Tag.MediaStorageSOPInstanceUID)))
        .getOrElse(throw PatchError("no SOPInstanceUID"))
      apply(ds, patch)
      val out = ByteArrayOutputStream()
      val dos = DicomOutputStream(out, UID.ExplicitVRLittleEndian)
      dos.writeDataset(fmi, ds)
      val atPixelData = dis.tag() == Tag.PixelData
      if atPixelData then dos.writeHeader(Tag.PixelData, dis.vr(), dis.length())
      dos.flush()
      PatchedInstance(sop, ts, out.toByteArray, if atPixelData then dis else { dis.close(); InputStream.nullInputStream() })
    catch
      case e: Throwable =>
        scala.util.Try(dis.close())
        throw e

  private def apply(ds: Attributes, p: PatientPatch): Unit =
    def set(tag: Int, vr: VR, v: Option[String]): Unit = v.filter(_.nonEmpty) match
      case Some(s) => ds.setString(tag, vr, s)
      case None => ds.setNull(tag, vr)
    set(Tag.PatientName, VR.PN, p.patientName)
    set(Tag.PatientID, VR.LO, Some(p.patientId))
    p.issuerOfPatientId.foreach(i => set(Tag.IssuerOfPatientID, VR.LO, Some(i)))
    set(Tag.PatientBirthDate, VR.DA, p.birthDate)
    set(Tag.PatientSex, VR.CS, p.sex)
    set(Tag.AccessionNumber, VR.SH, p.accessionNumber)
