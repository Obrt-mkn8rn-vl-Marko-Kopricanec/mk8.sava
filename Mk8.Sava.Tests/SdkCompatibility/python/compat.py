from __future__ import annotations

import base64
import os
import uuid
from datetime import datetime, timedelta, timezone
from importlib.metadata import version

from azure.storage.blob import (
    BlobBlock,
    BlobLeaseClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
)


endpoint = os.environ["MK8_SAVA_BLOB_ENDPOINT"]
account_name = os.environ["MK8_SAVA_ACCOUNT_NAME"]
account_key = os.environ["MK8_SAVA_ACCOUNT_KEY"]
service = BlobServiceClient(
    account_url=endpoint,
    credential=account_key,
    retry_total=0,
)
container_name = f"python-{uuid.uuid4().hex}"
container = service.get_container_client(container_name)
created = False

try:
    container.create_container(metadata={"runtime": "python"})
    created = True

    containers = list(
        service.list_containers(
            name_starts_with=container_name,
            include_metadata=True,
        )
    )
    assert len(containers) == 1
    assert containers[0]["name"] == container_name
    assert containers[0]["metadata"]["runtime"] == "python"

    content = bytes(
        (index * 31 + index // 263) % 251
        for index in range(256 * 1024 + 41)
    )
    block = container.get_blob_client("folder/sdk-roundtrip.bin")
    block.upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="application/x-mk8-python",
            cache_control="private,max-age=19",
        ),
        metadata={"owner": "python-sdk"},
        tags={"runtime": "python", "purpose": "compatibility"},
    )
    assert block.download_blob().readall() == content
    assert block.download_blob(offset=12_345, length=7_777).readall() == content[12_345:20_122]

    properties = block.get_blob_properties()
    assert properties.size == len(content)
    assert properties.content_settings.content_type == "application/x-mk8-python"
    assert properties.content_settings.cache_control == "private,max-age=19"
    assert properties.metadata["owner"] == "python-sdk"
    assert block.get_blob_tags()["runtime"] == "python"

    listed = list(
        container.list_blobs(
            name_starts_with="folder/",
            include=["metadata", "tags"],
        )
    )
    assert len(listed) == 1
    assert listed[0].name == block.blob_name
    assert listed[0].metadata["owner"] == "python-sdk"
    assert listed[0].tags["runtime"] == "python"

    snapshot = block.create_snapshot(metadata={"owner": "snapshot"})
    snapshot_client = container.get_blob_client(
        block.blob_name,
        snapshot=snapshot["snapshot"],
    )
    assert snapshot_client.download_blob().readall() == content
    assert snapshot_client.get_blob_properties().metadata["owner"] == "snapshot"

    staged = container.get_blob_client("staged.bin")
    block_ids = [
        base64.b64encode(value).decode("ascii")
        for value in (b"block-0001", b"block-0002")
    ]
    first = b"first-block|"
    second = b"second-block"
    staged.stage_block(block_id=block_ids[0], data=first)
    staged.stage_block(block_id=block_ids[1], data=second)
    staged.commit_block_list(
        [BlobBlock(block_id=value) for value in block_ids],
        metadata={"route": "staged"},
        tags={"runtime": "python"},
    )
    assert staged.download_blob().readall() == first + second
    committed, uncommitted = staged.get_block_list(block_list_type="committed")
    assert [item.id for item in committed] == block_ids
    assert uncommitted == []

    append = container.get_blob_client("append.log")
    append.create_append_blob()
    append.append_block(b"alpha")
    append.append_block(b"|beta")
    assert append.download_blob().readall() == b"alpha|beta"

    page = container.get_blob_client("page.bin")
    page.create_page_blob(size=1024)
    page_bytes = bytes([0x5A]) * 512
    page.upload_page(page=page_bytes, offset=0, length=len(page_bytes))
    page_ranges = list(page.list_page_ranges())
    assert len(page_ranges) == 1
    assert page_ranges[0].start == 0
    assert page_ranges[0].end == 511
    assert not page_ranges[0].cleared
    assert page.download_blob(offset=0, length=512).readall() == page_bytes

    lease = BlobLeaseClient(client=block)
    lease.acquire(lease_duration=-1)
    assert lease.id is not None
    lease.release()

    now = datetime.now(timezone.utc)
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=block.blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        start=now - timedelta(minutes=1),
        expiry=now + timedelta(minutes=10),
    )
    sas_blob = block.from_blob_url(f"{block.url}?{sas}", retry_total=0)
    assert sas_blob.download_blob().readall() == content

    print(
        f"azure-storage-blob {version('azure-storage-blob')} compatibility passed "
        "for block, staged, append, page, snapshot, lease, list, tags, ranges, and SAS operations."
    )
finally:
    if created:
        try:
            container.delete_container()
        except Exception:
            pass
