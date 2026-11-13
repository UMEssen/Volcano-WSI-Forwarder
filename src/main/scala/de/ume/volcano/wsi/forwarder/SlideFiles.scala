package de.ume.volcano.wsi.forwarder

import com.hierynomus.msdtyp.AccessMask
import com.hierynomus.mssmb2.{SMB2CreateDisposition, SMB2ShareAccess}
import com.hierynomus.smbj.{SMBClient, SmbConfig}
import com.hierynomus.smbj.auth.AuthenticationContext
import com.hierynomus.smbj.connection.Connection
import com.hierynomus.smbj.session.Session
import com.hierynomus.smbj.share.DiskShare
import java.io.{FilterInputStream, InputStream}
import java.util.EnumSet
import java.util.concurrent.TimeUnit
import org.dcm4che3.data.Tag
import org.dcm4che3.io.DicomInputStream

/** Read-only, sequential access to the scanner's shares. Paths are share-relative, `/`-separated. */
trait SlideFiles extends AutoCloseable:
  /** Open a file for one sequential read; the caller closes the stream. */
  def open(share: String, path: String): InputStream

final case class ShareLogin(user: String, password: String, domain: String)

/** SMB2/3 via smbj, one connection per share, re-established after a failure. */
final class SmbSlideFiles(server: String, logins: Map[String, ShareLogin], timeoutSeconds: Int = 60) extends SlideFiles:
  private val (host, port) = server.split(':') match
    case Array(h, p) => (h, p.toInt)
    case _ => (server, 445)
  // No socket read timeout on idle connections: requests carry their own timeout, and an idle
  // connection the server dropped is detected (isConnected) and replaced before the next use.
  private val client = SMBClient(SmbConfig.builder().withTimeout(timeoutSeconds.toLong, TimeUnit.SECONDS)
    .withSoTimeout(0L, TimeUnit.SECONDS).build())
  private val conns = scala.collection.mutable.Map.empty[String, (Connection, Session, DiskShare)]

  private def disk(share: String): DiskShare = synchronized {
    conns.get(share) match
      case Some((c, _, s)) if c.isConnected && s.isConnected => s
      case old =>
        old.foreach((c, _, _) => scala.util.Try(c.close(true)))
        val login = logins.getOrElse(share, throw IllegalArgumentException(s"no login configured for share $share"))
        val c = client.connect(host, port)
        val session = c.authenticate(AuthenticationContext(login.user, login.password.toCharArray, login.domain))
        val s = session.connectShare(share).asInstanceOf[DiskShare]
        conns(share) = (c, session, s)
        s
  }

  def open(share: String, path: String): InputStream =
    val smbPath = path.split('/').filter(_.nonEmpty).mkString("\\")
    try
      val f = disk(share).openFile(smbPath, EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null)
      new FilterInputStream(f.getInputStream):
        override def close(): Unit = try super.close() finally f.close()
    catch
      case e: Exception =>
        synchronized { conns.remove(share).foreach((c, _, _) => scala.util.Try(c.close(true))) }
        throw e

  def close(): Unit = synchronized {
    conns.values.foreach((c, _, _) => scala.util.Try(c.close(true)))
    conns.clear()
    client.close()
  }

/** The DICOMDIR's IMAGE records: SOP Instance UID → file, relative to the slide folder. */
object DicomDirIndex:
  def read(in: InputStream): Map[String, String] =
    val dis = DicomInputStream(in)
    try
      val ds = dis.readDataset()
      val records = Option(ds.getSequence(Tag.DirectoryRecordSequence)).map(s => (0 until s.size).map(s.get)).getOrElse(Seq.empty)
      records.filter(_.getString(Tag.DirectoryRecordType) == "IMAGE").flatMap { r =>
        for
          sop <- Option(r.getString(Tag.ReferencedSOPInstanceUIDInFile)).map(_.trim).filter(_.nonEmpty)
          file <- Option(r.getStrings(Tag.ReferencedFileID)).map(_.mkString("/")).filter(_.nonEmpty)
        yield sop -> file
      }.toMap
    finally dis.close()
