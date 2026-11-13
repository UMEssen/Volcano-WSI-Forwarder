# Volcano WSI Forwarder

Moves whole-slide images (DICOM WSI from the pathology scanners) into the research PACS, with the
patient's data written into every file, and hands the slide's FHIR `ImagingStudy` on with a
DICOMweb endpoint beside its SMB one. A stage of the [Volcano](https://github.com/UMEssen/Volcano)
pipeline; the decision record is [ADR 0013](https://github.com/UMEssen/Volcano/blob/main/docs/adr/0013-wsi-forwarder-to-research-pacs.md).

```
Volcano-SMB-Connector ─► WSI transformer ─► volcano.transformer.fhir.r4.wsi
                                                     │  one ImagingStudy bundle per slide
                                                     ▼
                                    volcano-wsi-forwarder (this service)
          SMB share ──files──►  patch patient module ──STOW-RS──►  research PACS (DICOM receiver)
          Fuego ──Patient────►
                                                     │
                       volcano.forwarder.fhir.r4.wsi ─► normaliser ─► sink ─► Fuego
                       volcano.forwarder.fhir.r4.wsi.errors  (failure records, contract v1)
```

Overview graphic of the whole WSI import pipeline, from the SMB shares to the research PACS and
Fuego, with the status of each piece: [WSI Import Pipeline](https://claude.ai/artifact/RT1oj4CBpKUhuKcyErc1VZ)
(access has to be granted by the owner).

## What it does, per slide

1. Reads the transformer's bundle: Study Instance UID, every SOP Instance UID, the SMB folder
   (the contained `smb` Endpoint), the subject's Medico person number and the case (E-number)
   from `basedOn`.
2. Reads the patient from Fuego by the Medico identifier (`Patient?identifier=…`, read-only).
3. Reads the slide's `DICOMDIR` to find each instance's file.
4. For each instance: opens the file on the share, rewrites the header with the patient module
   (`PatientName`, `PatientID` = Medico number, `IssuerOfPatientID`, `PatientBirthDate`,
   `PatientSex`, `AccessionNumber` = E-number) and streams the rest of the file unchanged into a
   STOW-RS request: one `application/dicom` part per request, chunked transfer encoding.
5. Emits the bundle with a second contained Endpoint (`dicom-wado-rs`, the research PACS's
   DICOMweb base) and an `endpoint` reference to it. The SMB endpoint stays, as provenance.

**Nothing is held in memory but the header.** dcm4che reads each file up to the pixel data and
stops right after that element's header; the header is re-encoded with the patch, and the pixel
data and everything after it are passed through as raw bytes. Pixel data and every UID stay
byte-identical; tests assert this for native and encapsulated pixel data, trailing padding and a
multi-MB ICC profile in the header. Deflated transfer syntaxes cannot be split this way and are
refused (`INVALID_DICOM`).

## Retries and ordering

One slide at a time per partition, in order, at least once (offsets are committed after the
output is written).

| Outcome | What happens |
|---|---|
| stored | enriched bundle to the sink topic, offset committed |
| transient failure | partition paused, the record re-read after an exponential backoff (`RETRY_BASE_SECONDS` … `RETRY_MAX_SECONDS`); the first attempt writes a non-final failure record |
| permanent failure, or transient for longer than `MAX_WAIT_SECONDS` | final failure record, and the **original** bundle goes to the sink unchanged (SMB endpoint only), so the FHIR resource is never held back longer than that; a replay adds the PACS endpoint later |

Pausing instead of sleeping keeps the consumer polling, so a waiting slide costs neither the group
membership nor the other partitions. A resend re-stores the same SOP Instance UIDs, which the
receiver overwrites under the same key.

| `volcano.reason` | Meaning | Retried |
|---|---|---|
| `PATIENT_LOOKUP_FAILED` | the Fuego search failed or the identifier is ambiguous | yes |
| `PATIENT_UNRESOLVED` | no patient with that Medico identifier | yes |
| `SMB_READ_FAILED` | the share could not be read | yes |
| `INSTANCE_MISSING` | an instance of the bundle is not in the `DICOMDIR` | yes |
| `STOW_FAILED` | network, 5xx, 408/429, or 409 (the receiver's storage failed) | yes |
| `STOW_REFUSED` | any other 4xx (e.g. 413: instance too large for the receiver) | no |
| `INVALID_DICOM` | not a PS3.10 file, deflated, or the SOP UID differs from the `DICOMDIR` | no |
| `NO_SUBJECT` | the bundle's subject has no Medico identifier | no |
| `BAD_RECORD` | not a WSI bundle (not forwarded at all) | no |

Failure records follow Volcano's failure-record contract v1 (`volcano.stage=forwarder`) and are
redacted by construction: the reason and a short technical detail, no slide id, UID, path or
patient value.

## Configuration

| Variable | Default | |
|---|---|---|
| `SOURCE_TOPIC` | `volcano.transformer.fhir.r4.wsi` | the transformer's output |
| `SINK_TOPIC` | `volcano.forwarder.fhir.r4.wsi` | |
| `ERRORS_TOPIC` | `<SINK_TOPIC>.errors` | |
| `KAFKA_GROUP_ID` | `volcano-forwarder-wsi` | |
| `KAFKA_BOOTSTRAP_SERVERS` | – | |
| `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_CA_FILE` | – | SCRAM-SHA-512 over TLS when set; CA as PEM |
| `SMB_SERVER` | – | e.g. `nas.example.org` or `host:port` |
| `SMB_SHARES` | – | comma list; per share `SMB_<SHARE>_USER`, `SMB_<SHARE>_PASSWORD`, optional `SMB_<SHARE>_DOMAIN` (share upper-cased, other characters as `_`) |
| `FUEGO_BASE_URL` | – | e.g. `https://…/fhir` |
| `OIDC_TOKEN_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_SCOPE` | – | client credentials (`client_secret_basic`); needs `system/Patient.rs` |
| `STOW_URL` | – | the receiver's STOW-RS endpoint, e.g. `http://receiver:8080/dicom-web/studies` |
| `STOW_TIMEOUT_SECONDS` | `3600` | per instance |
| `DICOMWEB_BASE_URL` | – | written as the PACS Endpoint's `address` |
| `ISSUER_OF_PATIENT_ID` | – | written as `IssuerOfPatientID` when set |
| `MAX_BYTES_PER_SECOND` | `0` (unlimited) | read budget towards the share, shared by all transfers of the process |
| `RETRY_BASE_SECONDS`, `RETRY_MAX_SECONDS` | `60`, `3600` | backoff for transient failures |
| `MAX_WAIT_SECONDS` | `604800` (7 d) | then forward without the PACS endpoint |
| `HEALTH_PORT` | `8080` | `GET /health`: 200 while the loop polls |
| `VOLCANO_RELEASE` | `dev` | stamped into failure records |

## Build, test, run

```bash
sbt test            # unit tests: patching, STOW client, Fuego lookup, the consumer loop (MockConsumer)
sbt assembly        # target/scala-3.3.6/volcano-wsi-forwarder.jar
docker build -f Containerfile .
```

Against a real receiver (opt-in, synthetic instances only):

```bash
RECEIVER_STOW_URL=http://localhost:8080/dicom-web/studies sbt "testOnly *ReceiverInteropSpec"
```

Checked on 2026-11-13 against the Firemetrics DICOM receiver (`edd26c4d`, with Postgres and an S3
store): both instances stored, the patched patient module and accession number in the stored
objects and in the study metadata, pixel data complete.

## Limits

- **Instance size at the receiver.** The released receiver's STOW-RS buffers each instance in
  memory with a 100 MiB request limit; multi-GB full-resolution levels need its streaming STOW,
  [Firemetrics/firemetrics#1134](https://github.com/Firemetrics/firemetrics/pull/1134) ("stream
  STOW-RS instances into S3 with bounded memory", open; plan:
  `dicom_receiver/plans/stow-streaming.md`). Until a receiver with it is deployed, such an instance
  fails with `STOW_REFUSED` (413) or, if the receiver runs out of memory, `STOW_FAILED`.
- **No "already stored" check yet.** The receiver has no QIDO-RS; every attempt sends every
  instance of the slide again. A check against the receiver's object store or a DICOMweb front is
  the next step for the backfill.
- **Patient data is a snapshot** of Fuego at transfer time.
