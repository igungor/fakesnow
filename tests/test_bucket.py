from __future__ import annotations

import base64
import hashlib
import os
import uuid

import boto3
import pytest
import requests
import snowflake.connector
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from mypy_boto3_s3 import S3Client

from fakesnow.bucket import STAGE_MASTER_KEY

BUCKET = "fs_bucket"


@pytest.fixture
def s3(server: dict) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=f"http://{server['host']}:{server['port']}",
        region_name="us-west-2",
        aws_access_key_id="fake",
        aws_secret_access_key="fake",
    )


@pytest.fixture
def prefix() -> str:
    # the bucket is shared by the tests' servers, so each test uses its own prefix
    return f"DB/SCHEMA/{uuid.uuid4().hex.upper()}/"


def test_put_head_get(s3: S3Client, prefix: str) -> None:
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}a.json", Body=b"{}", Metadata={"sfc-digest": "abc"})

    head = s3.head_object(Bucket=BUCKET, Key=f"{prefix}a.json")
    assert head["ContentLength"] == 2
    assert head["ETag"] == f'"{hashlib.md5(b"{}").hexdigest()}"'
    assert head["Metadata"] == {"sfc-digest": "abc"}

    get = s3.get_object(Bucket=BUCKET, Key=f"{prefix}a.json")
    assert get["Body"].read() == b"{}"
    assert get["Metadata"] == {"sfc-digest": "abc"}


def test_get_range(s3: S3Client, prefix: str) -> None:
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}a.txt", Body=b"0123456789")

    assert s3.get_object(Bucket=BUCKET, Key=f"{prefix}a.txt", Range="bytes=2-4")["Body"].read() == b"234"
    assert s3.get_object(Bucket=BUCKET, Key=f"{prefix}a.txt", Range="bytes=7-")["Body"].read() == b"789"
    assert s3.get_object(Bucket=BUCKET, Key=f"{prefix}a.txt", Range="bytes=-2")["Body"].read() == b"89"


def test_missing_object(s3: S3Client, prefix: str) -> None:
    with pytest.raises(ClientError) as excinfo:
        s3.get_object(Bucket=BUCKET, Key=f"{prefix}missing")
    assert excinfo.value.response["Error"]["Code"] == "NoSuchKey"

    with pytest.raises(ClientError) as excinfo:
        s3.head_object(Bucket=BUCKET, Key=f"{prefix}missing")
    assert excinfo.value.response["Error"]["Code"] == "404"


def test_delete(s3: S3Client, prefix: str) -> None:
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}a", Body=b"a")
    s3.delete_object(Bucket=BUCKET, Key=f"{prefix}a")
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)["KeyCount"] == 0

    # deleting a missing object succeeds
    s3.delete_object(Bucket=BUCKET, Key=f"{prefix}a")


def test_list_objects(s3: S3Client, prefix: str) -> None:
    for name in ["b", "a", "dir/c"]:
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}{name}", Body=name.encode())

    listed = s3.list_objects(Bucket=BUCKET, Prefix=prefix)
    assert [(o["Key"], o["Size"]) for o in listed["Contents"]] == [
        (f"{prefix}a", 1),
        (f"{prefix}b", 1),
        (f"{prefix}dir/c", 5),
    ]
    assert listed["Contents"][0]["ETag"] == f'"{hashlib.md5(b"a").hexdigest()}"'

    listed_v2 = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefix}dir/")
    assert [o["Key"] for o in listed_v2["Contents"]] == [f"{prefix}dir/c"]


def test_list_objects_url_encoded(s3: S3Client, prefix: str) -> None:
    # a table stage's directory starts with %, which the JDBC driver's S3 client asks to be url-encoded
    key = f"{prefix}%TABLE/a b.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    listed = s3.list_objects(Bucket=BUCKET, Prefix=f"{prefix}%TABLE/", EncodingType="url")
    assert listed["EncodingType"] == "url"
    assert [o["Key"] for o in listed["Contents"]] == [key.replace("%", "%25").replace(" ", "%20")]


def test_list_objects_pages(s3: S3Client, prefix: str) -> None:
    for name in ["a", "b", "c"]:
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}{name}", Body=b"x")

    first = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=2)
    assert [o["Key"] for o in first["Contents"]] == [f"{prefix}a", f"{prefix}b"]
    assert first["IsTruncated"] is True

    second = s3.list_objects_v2(
        Bucket=BUCKET, Prefix=prefix, MaxKeys=2, ContinuationToken=first["NextContinuationToken"]
    )
    assert [o["Key"] for o in second["Contents"]] == [f"{prefix}c"]
    assert second["IsTruncated"] is False

    # ListObjects pages with a marker
    pages = [s3.list_objects(Bucket=BUCKET, Prefix=prefix, MaxKeys=2)]
    pages.append(s3.list_objects(Bucket=BUCKET, Prefix=prefix, MaxKeys=2, Marker=pages[0]["NextMarker"]))
    assert [[o["Key"] for o in page["Contents"]] for page in pages] == [
        [f"{prefix}a", f"{prefix}b"],
        [f"{prefix}c"],
    ]


def test_multipart_upload(s3: S3Client, prefix: str, tmp_path: os.PathLike) -> None:
    content = os.urandom(12 * 1024 * 1024)
    path = os.path.join(tmp_path, "big")
    with open(path, "wb") as f:
        f.write(content)

    config = TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024)
    s3.upload_file(path, BUCKET, f"{prefix}big", Config=config, ExtraArgs={"Metadata": {"sfc-digest": "d"}})

    head = s3.head_object(Bucket=BUCKET, Key=f"{prefix}big")
    assert head["ContentLength"] == len(content)
    assert head["ETag"].endswith('-3"')
    assert head["Metadata"] == {"sfc-digest": "d"}
    assert s3.get_object(Bucket=BUCKET, Key=f"{prefix}big")["Body"].read() == content


def test_abort_multipart_upload(s3: S3Client, prefix: str) -> None:
    upload = s3.create_multipart_upload(Bucket=BUCKET, Key=f"{prefix}aborted")
    s3.upload_part(Bucket=BUCKET, Key=f"{prefix}aborted", UploadId=upload["UploadId"], PartNumber=1, Body=b"x")
    s3.abort_multipart_upload(Bucket=BUCKET, Key=f"{prefix}aborted", UploadId=upload["UploadId"])

    with pytest.raises(ClientError) as excinfo:
        s3.complete_multipart_upload(
            Bucket=BUCKET, Key=f"{prefix}aborted", UploadId=upload["UploadId"], MultipartUpload={"Parts": []}
        )
    assert excinfo.value.response["Error"]["Code"] == "NoSuchUpload"
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)["KeyCount"] == 0


def test_key_outside_bucket(server: dict) -> None:
    response = requests.put(f"http://{server['host']}:{server['port']}/fs_bucket/%2E%2E/escaped", data=b"x", timeout=5)
    assert response.status_code == 400


def _encrypt(content: bytes) -> tuple[bytes, dict[str, str]]:
    """Encrypt a file for an internal stage as snowflake's clients do, returning it and its S3 metadata."""

    def pad(data: bytes) -> bytes:
        padder = padding.PKCS7(128).padder()
        return padder.update(data) + padder.finalize()

    file_key, iv = os.urandom(16), os.urandom(16)
    encryptor = Cipher(algorithms.AES(file_key), modes.CBC(iv)).encryptor()
    body = encryptor.update(pad(content)) + encryptor.finalize()
    key_encryptor = Cipher(algorithms.AES(STAGE_MASTER_KEY), modes.ECB()).encryptor()
    wrapped = key_encryptor.update(pad(file_key)) + key_encryptor.finalize()
    return body, {"x-amz-key": base64.b64encode(wrapped).decode(), "x-amz-iv": base64.b64encode(iv).decode()}


def test_copy_multipart_encrypted(server: dict, s3: S3Client) -> None:
    with snowflake.connector.connect(**server, database="db1", schema="schema1") as conn, conn.cursor() as cur:
        cur.execute("CREATE STAGE multipart_stage")
        cur.execute("CREATE TABLE multipart_table (a INT)")

        rows = b"".join(f"{i}\n".encode() for i in range(1_500_000))
        body, metadata = _encrypt(rows)
        key = "DB1/SCHEMA1/MULTIPART_STAGE/rows.csv"
        upload = s3.create_multipart_upload(Bucket=BUCKET, Key=key, Metadata=metadata)
        part_size = 5 * 1024 * 1024
        parts = []
        for number, start in enumerate(range(0, len(body), part_size), start=1):
            part = s3.upload_part(
                Bucket=BUCKET,
                Key=key,
                UploadId=upload["UploadId"],
                PartNumber=number,
                Body=body[start : start + part_size],
            )
            parts.append({"PartNumber": number, "ETag": part["ETag"]})
        s3.complete_multipart_upload(
            Bucket=BUCKET, Key=key, UploadId=upload["UploadId"], MultipartUpload={"Parts": parts}
        )

        cur.execute("COPY INTO multipart_table FROM @multipart_stage")
        cur.execute("SELECT count(*), max(a) FROM multipart_table")
        assert cur.fetchall() == [(1_500_000, 1_499_999)]
