package de.ume.volcano.wsi.forwarder

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, SequenceInputStream}
import org.dcm4che3.data.{Attributes, Fragments, Tag, UID, VR}
import org.dcm4che3.io.{DicomInputStream, DicomOutputStream}
import org.scalatest.funsuite.AnyFunSuite

/** Synthetic instances only: invented UIDs under 2.25, no patient data. */
object Synthetic:
  val Wsi = "1.2.840.10008.5.1.4.1.1.77.1.6" // VL Whole Slide Microscopy Image Storage
  val JpegBaseline = UID.JPEGBaseline8Bit

  def header(sop: String, iccBytes: Int = 0): Attributes =
    val ds = Attributes()
    ds.setString(Tag.SOPClassUID, VR.UI, Wsi)
    ds.setString(Tag.SOPInstanceUID, VR.UI, sop)
    ds.setString(Tag.StudyInstanceUID, VR.UI, "2.25.1001")
    ds.setString(Tag.SeriesInstanceUID, VR.UI, "2.25.1002")
    ds.setNull(Tag.PatientName, VR.PN)
    ds.setNull(Tag.PatientID, VR.LO)
    ds.setNull(Tag.PatientBirthDate, VR.DA)
    ds.setNull(Tag.PatientSex, VR.CS)
    ds.setNull(Tag.AccessionNumber, VR.SH)
    ds.setString(Tag.ContainerIdentifier, VR.LO, "E1_99-1A.1")
    ds.setString(Tag.ImageType, VR.CS, "ORIGINAL", "PRIMARY", "VOLUME", "NONE")
    if iccBytes > 0 then ds.setBytes(Tag.ICCProfile, VR.OB, Array.tabulate[Byte](iccBytes)(i => (i % 251).toByte))
    ds.setInt(Tag.Rows, VR.US, 4)
    ds.setInt(Tag.Columns, VR.US, 4)
    ds

  /** A PS3.10 file; `encapsulated` = JPEG baseline fragments, else native OW pixel data. */
  def file(sop: String, pixels: Option[Array[Byte]], encapsulated: Boolean = true, iccBytes: Int = 0, trailingPadding: Boolean = false): Array[Byte] =
    val ds = header(sop, iccBytes)
    val ts = if encapsulated then JpegBaseline else UID.ExplicitVRLittleEndian
    pixels.foreach { px =>
      if encapsulated then
        val frags = ds.newFragments(Tag.PixelData, VR.OB, 3)
        frags.add(Array.emptyByteArray) // Basic Offset Table
        frags.add(px.take(px.length / 2))
        frags.add(px.drop(px.length / 2))
      else ds.setBytes(Tag.PixelData, VR.OW, px)
    }
    if trailingPadding then ds.setBytes(Tag.DataSetTrailingPadding, VR.OB, Array.fill[Byte](8)(0))
    val out = ByteArrayOutputStream()
    val dos = DicomOutputStream(out, UID.ExplicitVRLittleEndian)
    dos.writeDataset(ds.createFileMetaInformation(ts), ds)
    dos.close()
    out.toByteArray

  def read(bytes: Array[Byte]): (Attributes, Attributes) =
    val dis = DicomInputStream(ByteArrayInputStream(bytes))
    try (dis.readFileMetaInformation(), dis.readDataset()) finally dis.close()

class PatcherSpec extends AnyFunSuite:
  import Synthetic.*

  private val patch = PatientPatch(Some("Doe^Jane"), "4711", Some("UME"), Some("19700101"), Some("F"), Some("E1/99"))

  private def patched(original: Array[Byte]): (PatchedInstance, Array[Byte]) =
    val p = Patcher.open(ByteArrayInputStream(original), patch)
    val all = SequenceInputStream(ByteArrayInputStream(p.head), p.rest).readAllBytes()
    (p, all)

  private def patientTags = Seq(Tag.PatientName, Tag.PatientID, Tag.IssuerOfPatientID, Tag.PatientBirthDate, Tag.PatientSex, Tag.AccessionNumber)

  private def assertOnlyPatientChanged(original: Array[Byte], result: Array[Byte]): Unit =
    val (_, before) = read(original)
    val (fmi, after) = read(result)
    assert(after.getString(Tag.PatientName) == "Doe^Jane")
    assert(after.getString(Tag.PatientID) == "4711")
    assert(after.getString(Tag.IssuerOfPatientID) == "UME")
    assert(after.getString(Tag.PatientBirthDate) == "19700101")
    assert(after.getString(Tag.PatientSex) == "F")
    assert(after.getString(Tag.AccessionNumber) == "E1/99")
    val b = Attributes(before); val a = Attributes(after)
    patientTags.foreach { t => b.remove(t); a.remove(t) }
    assert(a == b, "everything but the patient module is unchanged")
    assert(fmi.getString(Tag.MediaStorageSOPInstanceUID) == before.getString(Tag.SOPInstanceUID))

  test("encapsulated pixel data: header patched, fragments byte-identical, rest streamed raw") {
    val px = Array.tabulate[Byte](10_000)(i => (i * 7).toByte)
    val original = file("2.25.1", Some(px))
    val (p, result) = patched(original)
    assert(p.sopInstanceUid == "2.25.1" && p.transferSyntax == JpegBaseline)
    assertOnlyPatientChanged(original, result)
    val (_, after) = read(result)
    val frags = after.getValue(Tag.PixelData).asInstanceOf[Fragments]
    assert(frags.size == 3)
    assert(frags.get(1).asInstanceOf[Array[Byte]].sameElements(px.take(5000)))
    assert(frags.get(2).asInstanceOf[Array[Byte]].sameElements(px.drop(5000)))
    // The pixel data element is passed through untouched: the original's tail equals the result's tail.
    val tail = original.length - original.indexOfSlice(Array[Byte](0xE0.toByte, 0x7F, 0x10, 0x00))
    assert(result.takeRight(tail).sameElements(original.takeRight(tail)))
  }

  test("native pixel data and trailing padding after it pass through") {
    val px = Array.tabulate[Byte](4 * 4 * 2)(_.toByte)
    val original = file("2.25.2", Some(px), encapsulated = false, trailingPadding = true)
    val (_, result) = patched(original)
    assertOnlyPatientChanged(original, result)
    assert(read(result)._2.getBytes(Tag.PixelData).sameElements(px))
  }

  test("a large value before the pixel data (ICC profile) stays in the header") {
    val original = file("2.25.3", Some(Array.fill[Byte](64)(1)), iccBytes = 3_000_000)
    val (p, result) = patched(original)
    assert(p.head.length > 3_000_000)
    assertOnlyPatientChanged(original, result)
  }

  test("no pixel data: the whole instance is the header") {
    val original = file("2.25.4", None)
    val (p, result) = patched(original)
    assert(result.sameElements(p.head), "nothing follows the header")
    assertOnlyPatientChanged(original, result)
  }

  test("a missing value is written empty, not left over from the scanner") {
    val p = Patcher.open(ByteArrayInputStream(file("2.25.5", None)), patch.copy(patientName = None, birthDate = None, sex = None, accessionNumber = None, issuerOfPatientId = None))
    val (_, ds) = read(p.head)
    assert(ds.containsValue(Tag.PatientID) && !ds.containsValue(Tag.PatientName) && ds.contains(Tag.PatientName))
    assert(!ds.contains(Tag.IssuerOfPatientID))
  }

  test("not a PS3.10 file is refused") {
    val ds = header("2.25.6")
    val out = ByteArrayOutputStream()
    val dos = DicomOutputStream(out, UID.ImplicitVRLittleEndian)
    dos.writeDataset(null, ds); dos.close()
    assertThrows[PatchError](Patcher.open(ByteArrayInputStream(out.toByteArray), patch))
  }
