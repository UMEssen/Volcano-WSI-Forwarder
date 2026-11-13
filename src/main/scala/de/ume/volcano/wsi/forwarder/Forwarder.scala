package de.ume.volcano.wsi.forwarder

import org.slf4j.LoggerFactory

/** Failure reasons, as `volcano.reason` on the errors tier. */
enum Reason(val label: String, val transient: Boolean):
  case BadRecord extends Reason("BAD_RECORD", false)
  case NoSubject extends Reason("NO_SUBJECT", false)
  case PatientLookupFailed extends Reason("PATIENT_LOOKUP_FAILED", true)
  case PatientUnresolved extends Reason("PATIENT_UNRESOLVED", true)
  case SmbReadFailed extends Reason("SMB_READ_FAILED", true)
  case InstanceMissing extends Reason("INSTANCE_MISSING", true)
  case InvalidDicom extends Reason("INVALID_DICOM", false)
  case StowFailed extends Reason("STOW_FAILED", true)
  case StowRefused extends Reason("STOW_REFUSED", false)

final case class Failure(reason: Reason, detail: String)

/** One slide: read the patient, stream every instance from the share into the research PACS with
  * the patient module patched in, and return the bundle with the PACS endpoint added.
  */
final class Forwarder(
    files: SlideFiles,
    patients: PatientSource,
    sink: InstanceSink,
    limiter: RateLimiter,
    dicomwebBase: String,
    issuerOfPatientId: Option[String]
):
  private val log = LoggerFactory.getLogger(classOf[Forwarder])

  def forward(r: StudyRecord): Either[Failure, String] =
    for
      medico <- r.medicoId.toRight(Failure(Reason.NoSubject, "subject carries no Medico identifier"))
      demo <- patients.demographics(medico) match
        case Left(err) => Left(Failure(Reason.PatientLookupFailed, err))
        case Right(None) => Left(Failure(Reason.PatientUnresolved, "no patient with that Medico identifier"))
        case Right(Some(d)) => Right(d)
      index <- readIndex(r)
      patch = PatientPatch(demo.patientName, medico, issuerOfPatientId, demo.birthDate, demo.sex, r.caseNumber)
      _ <- sendAll(r, index, patch)
    yield
      log.info("slide stored in the research PACS: {} instance(s), study {}", r.sopInstanceUids.size, r.studyInstanceUid)
      StudyRecord.withPacsEndpoint(r, dicomwebBase)

  private def path(r: StudyRecord, file: String) = s"${r.smb.folder}/$file"

  private def readIndex(r: StudyRecord): Either[Failure, Map[String, String]] =
    val index =
      try
        val in = files.open(r.smb.share, path(r, "DICOMDIR"))
        try DicomDirIndex.read(in) finally in.close()
      catch case e: Exception => return Left(Failure(Reason.SmbReadFailed, s"DICOMDIR: ${e.getClass.getSimpleName}"))
    val missing = r.sopInstanceUids.filterNot(index.contains)
    if missing.nonEmpty then Left(Failure(Reason.InstanceMissing, s"${missing.size} instance(s) not in the DICOMDIR"))
    else Right(index)

  private def sendAll(r: StudyRecord, index: Map[String, String], patch: PatientPatch): Either[Failure, Unit] =
    r.sopInstanceUids.foldLeft[Either[Failure, Unit]](Right(())) { (acc, sop) =>
      acc.flatMap(_ => send(r, sop, index(sop), patch))
    }

  private def send(r: StudyRecord, sop: String, file: String, patch: PatientPatch): Either[Failure, Unit] =
    val in =
      try RateLimitedInputStream(files.open(r.smb.share, path(r, file)), limiter)
      catch case e: Exception => return Left(Failure(Reason.SmbReadFailed, s"open: ${e.getClass.getSimpleName}"))
    val instance =
      try Patcher.open(in, patch)
      catch
        case e: PatchError => in.close(); return Left(Failure(Reason.InvalidDicom, e.getMessage))
        case e: java.io.IOException => in.close(); return Left(Failure(Reason.SmbReadFailed, s"read: ${e.getClass.getSimpleName}"))
        case e: Exception => in.close(); return Left(Failure(Reason.InvalidDicom, e.getClass.getSimpleName))
    if instance.sopInstanceUid != sop then
      instance.close()
      return Left(Failure(Reason.InvalidDicom, "the file's SOP Instance UID differs from the DICOMDIR"))
    sink.store(instance).left.map { f =>
      Failure(if f.transient then Reason.StowFailed else Reason.StowRefused, f.detail)
    }
