"""The bucket internal stages are stored in, served over http with the parts of the S3 and GCS APIs that
snowflake's clients use to upload to a stage.

Clients upload to a GCS presigned url, or with S3's PutObject or multipart upload. The S3 client in the JDBC
driver also lists the stage and reads each object's metadata to skip files already uploaded. Credentials and
signatures aren't checked.

See https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
import logging
import os
import secrets
import shutil
import tempfile
import uuid
from email.utils import formatdate
from urllib.parse import quote
from xml.etree import ElementTree

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import Headers, QueryParams
from starlette.requests import Request
from starlette.responses import Response

from fakesnow.transforms import stage

logger = logging.getLogger("fakesnow.server")

# an underscore isn't valid in a host name, so S3 clients address the bucket path-style, ie:
# http://host/fs_bucket/key rather than http://fs_bucket.host/key
BUCKET = "fs_bucket"

# clients encrypt files with a key wrapped by the stage master key before uploading them to an internal stage
STAGE_MASTER_KEY = secrets.token_bytes(16)
STAGE_MASTER_KEY_ID = 1

# multipart uploads in progress, by upload id
UPLOADS_PATH = tempfile.mkdtemp(prefix="fakesnow_uploads_")

S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
USER_METADATA_PREFIXES = ("x-amz-meta-", "x-goog-meta-")


def _target(key: str) -> str | None:
    """The path of an object in the local bucket, or None if the key escapes it."""
    target = os.path.normpath(os.path.join(stage.LOCAL_BUCKET_PATH, key))
    return target if target.startswith(stage.LOCAL_BUCKET_PATH + os.sep) else None


def _key(path: str) -> str:
    return os.path.relpath(path, stage.LOCAL_BUCKET_PATH)


def _unwrap(wrapped_key: bytes) -> bytes:
    decryptor = Cipher(algorithms.AES(STAGE_MASTER_KEY), modes.ECB()).decryptor()
    unpadder = padding.PKCS7(128).unpadder()
    padded = decryptor.update(wrapped_key) + decryptor.finalize()
    return unpadder.update(padded) + unpadder.finalize()


def _user_metadata(headers: Headers) -> dict[str, str]:
    return {
        name[len(prefix) :]: value
        for name, value in headers.items()
        for prefix in USER_METADATA_PREFIXES
        if name.lower().startswith(prefix)
    }


def _file_metadata(user_metadata: dict[str, str], etag: str) -> stage.FileMetadata:
    """The metadata to keep with an uploaded file, including the key it was encrypted with, if it was.

    Clients encrypt files for snowflake's internal stages with a random key, using AES-CBC, and send that key
    encrypted with the stage master key, using AES-ECB, in the object's metadata: GCS's encryptiondata, or S3's
    x-amz-key and x-amz-iv.
    """
    metadata: stage.FileMetadata = {"metadata": user_metadata, "etag": etag}
    if encryption_data := user_metadata.get("encryptiondata"):
        data = json.loads(encryption_data)
        wrapped_key = base64.b64decode(data["WrappedContentKey"]["EncryptedKey"])
        iv = base64.b64decode(data["ContentEncryptionIV"])
    elif (key := user_metadata.get("x-amz-key")) and (iv64 := user_metadata.get("x-amz-iv")):
        wrapped_key = base64.b64decode(key)
        iv = base64.b64decode(iv64)
    else:
        return metadata
    metadata["key"] = base64.b64encode(_unwrap(wrapped_key)).decode()
    metadata["iv"] = base64.b64encode(iv).decode()
    return metadata


def _decode_aws_chunked(body: bytes) -> bytes:
    """The payload of an S3 upload sent with aws-chunked content encoding.

    Each chunk is its size in hex, optionally followed by ;chunk-signature=..., then CRLF, the data and CRLF. A
    zero size chunk ends the payload, followed by any trailing headers, eg: a checksum.
    See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
    """
    payload = bytearray()
    position = 0
    while True:
        line_end = body.index(b"\r\n", position)
        size = int(body[position:line_end].split(b";", 1)[0], 16)
        if size == 0:
            return bytes(payload)
        start = line_end + 2
        payload += body[start : start + size]
        position = start + size + 2


async def _body(request: Request) -> bytes:
    body = await request.body()
    if request.headers.get("x-amz-content-sha256", "").startswith("STREAMING-"):
        body = _decode_aws_chunked(body)
    return body


def _error(status_code: int, code: str, message: str) -> Response:
    root = ElementTree.Element("Error")
    ElementTree.SubElement(root, "Code").text = code
    ElementTree.SubElement(root, "Message").text = message
    return _xml(root, status_code)


def _xml(root: ElementTree.Element, status_code: int = 200) -> Response:
    body = b'<?xml version="1.0" encoding="UTF-8"?>' + ElementTree.tostring(root)
    return Response(body, status_code=status_code, media_type="application/xml")


def _element(parent: ElementTree.Element, name: str, text: str | None = None) -> ElementTree.Element:
    element = ElementTree.SubElement(parent, name)
    if text is not None:
        element.text = text
    return element


async def bucket(request: Request) -> Response:
    """List the bucket's objects, with ListObjects or ListObjectsV2."""
    params = request.query_params
    v2 = params.get("list-type") == "2"
    return await run_in_threadpool(
        _list_objects,
        params.get("prefix", ""),
        int(params.get("max-keys", "1000")),
        (params.get("continuation-token") or params.get("start-after")) if v2 else params.get("marker"),
        v2,
        params.get("encoding-type") == "url",
    )


def _list_objects(prefix: str, max_keys: int, start_after: str | None, v2: bool, url_encoded: bool) -> Response:
    """List the objects whose key starts with prefix, after start_after, in key order.

    With url_encoded, keys in the response are url-encoded, as clients ask for when keys can have characters
    XML can't hold. The JDBC driver always does, and decodes them.
    """

    def encode(key: str) -> str:
        return quote(key, safe="/") if url_encoded else key

    keys = sorted(
        key
        for root, _dirs, files in os.walk(stage.LOCAL_BUCKET_PATH)
        for file in files
        if (key := _key(os.path.join(root, file))).startswith(prefix) and (not start_after or key > start_after)
    )
    page, truncated = keys[:max_keys], len(keys) > max_keys

    root = ElementTree.Element("ListBucketResult", xmlns=S3_NAMESPACE)
    _element(root, "Name", BUCKET)
    _element(root, "Prefix", encode(prefix))
    _element(root, "MaxKeys", str(max_keys))
    if url_encoded:
        _element(root, "EncodingType", "url")
    _element(root, "IsTruncated", "true" if truncated else "false")
    if v2:
        _element(root, "KeyCount", str(len(page)))
        if truncated:
            _element(root, "NextContinuationToken", page[-1])
    elif truncated:
        _element(root, "NextMarker", encode(page[-1]))
    for key in page:
        path = os.path.join(stage.LOCAL_BUCKET_PATH, key)
        contents = _element(root, "Contents")
        _element(contents, "Key", encode(key))
        _element(contents, "LastModified", _iso(os.path.getmtime(path)))
        _element(contents, "ETag", _etag(path))
        _element(contents, "Size", str(os.path.getsize(path)))
        _element(contents, "StorageClass", "STANDARD")
    return _xml(root)


def _etag(path: str) -> str:
    """The object's ETag: the md5 of its content, or of its parts' md5s for a multipart upload."""
    if metadata := stage.file_metadata(path):
        return metadata["etag"]
    with open(path, "rb") as f:
        return f'"{hashlib.md5(f.read()).hexdigest()}"'


def _iso(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


async def bucket_object(request: Request) -> Response:
    """An object in the bucket: S3's PutObject, GetObject, HeadObject, DeleteObject and multipart upload, and a
    PUT to a GCS presigned url."""
    key = request.path_params["path"]
    if not key:
        return await bucket(request) if request.method == "GET" else _error(405, "MethodNotAllowed", "")
    if not (target := _target(key)):
        return _error(400, "InvalidArgument", "Key escapes the bucket")
    body = await _body(request) if request.method in ("PUT", "POST") else b""
    return await run_in_threadpool(_object, request.method, key, target, request.query_params, request.headers, body)


def _object(method: str, key: str, target: str, params: QueryParams, headers: Headers, body: bytes) -> Response:
    if method == "POST" and "uploads" in params:
        return _create_multipart_upload(key, _user_metadata(headers))
    if upload_id := params.get("uploadId"):
        if not os.path.isdir(upload_dir := os.path.join(UPLOADS_PATH, os.path.basename(upload_id))):
            return _error(404, "NoSuchUpload", "The specified upload does not exist.")
        if method == "PUT":
            return _upload_part(upload_dir, int(params["partNumber"]), body)
        if method == "POST":
            return _complete_multipart_upload(upload_dir, key, target)
        shutil.rmtree(upload_dir)
        return Response(status_code=204)

    if method == "PUT":
        etag = f'"{hashlib.md5(body).hexdigest()}"'
        stage.write_file(target, body, _file_metadata(_user_metadata(headers), etag))
        logger.info(f"Uploaded {len(body)} bytes to {target}")
        # S3 clients check the upload against its md5, which S3 returns as the ETag of an unencrypted object
        return Response(headers={"ETag": etag})

    if not os.path.isfile(target):
        # deleting a missing object succeeds, as in S3
        if method == "DELETE":
            return Response(status_code=204)
        return _error(404, "NoSuchKey", "The specified key does not exist.")

    if method == "DELETE":
        stage.remove_file(target)
        return Response(status_code=204)

    object_headers = _object_headers(target)
    if method == "HEAD":
        return Response(headers=object_headers)
    return _get_object(target, object_headers, headers.get("range"))


def _object_headers(path: str) -> dict[str, str]:
    metadata = stage.file_metadata(path)
    headers = {
        "Content-Length": str(os.path.getsize(path)),
        "ETag": _etag(path),
        "Last-Modified": formatdate(os.path.getmtime(path), usegmt=True),
        "Accept-Ranges": "bytes",
    }
    for name, value in (metadata["metadata"] if metadata else {}).items():
        headers[f"x-amz-meta-{name}"] = value
    return headers


def _get_object(path: str, headers: dict[str, str], byte_range: str | None) -> Response:
    with open(path, "rb") as f:
        content = f.read()
    if not (byte_range and byte_range.startswith("bytes=")):
        return Response(content, headers=headers, media_type="application/octet-stream")

    first, _, last = byte_range[len("bytes=") :].partition("-")
    start = len(content) - int(last) if not first else int(first)
    end = len(content) - 1 if not first or not last else min(int(last), len(content) - 1)
    if start >= len(content) or start > end:
        return _error(416, "InvalidRange", "The requested range is not satisfiable")
    part = content[start : end + 1]
    headers = headers | {"Content-Length": str(len(part)), "Content-Range": f"bytes {start}-{end}/{len(content)}"}
    return Response(part, status_code=206, headers=headers, media_type="application/octet-stream")


def _create_multipart_upload(key: str, user_metadata: dict[str, str]) -> Response:
    upload_id = uuid.uuid4().hex
    upload_dir = os.path.join(UPLOADS_PATH, upload_id)
    os.makedirs(upload_dir)
    with open(os.path.join(upload_dir, "metadata.json"), "w") as f:
        json.dump(user_metadata, f)

    root = ElementTree.Element("InitiateMultipartUploadResult", xmlns=S3_NAMESPACE)
    _element(root, "Bucket", BUCKET)
    _element(root, "Key", key)
    _element(root, "UploadId", upload_id)
    return _xml(root)


def _upload_part(upload_dir: str, part_number: int, body: bytes) -> Response:
    with open(os.path.join(upload_dir, f"{part_number:05d}.part"), "wb") as f:
        f.write(body)
    return Response(headers={"ETag": f'"{hashlib.md5(body).hexdigest()}"'})


def _complete_multipart_upload(upload_dir: str, key: str, target: str) -> Response:
    parts = sorted(p for p in os.listdir(upload_dir) if p.endswith(".part"))
    content = bytearray()
    digests = b""
    for part in parts:
        with open(os.path.join(upload_dir, part), "rb") as f:
            data = f.read()
        content += data
        digests += hashlib.md5(data).digest()
    with open(os.path.join(upload_dir, "metadata.json")) as f:
        user_metadata = json.load(f)

    etag = f'"{hashlib.md5(digests).hexdigest()}-{len(parts)}"'
    stage.write_file(target, bytes(content), _file_metadata(user_metadata, etag))
    shutil.rmtree(upload_dir)
    logger.info(f"Uploaded {len(content)} bytes in {len(parts)} parts to {target}")

    root = ElementTree.Element("CompleteMultipartUploadResult", xmlns=S3_NAMESPACE)
    _element(root, "Bucket", BUCKET)
    _element(root, "Key", key)
    _element(root, "ETag", etag)
    return _xml(root)
