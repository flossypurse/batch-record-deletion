from __future__ import annotations

import asyncio
import json
import os
from random import randint
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

from .config import producer, consumer


async def delete_batch(ctx: Context, record_id: str, batch_size: int = 10) -> bool:
    print(f"deleting a batch of rows related to record {record_id}")
    if randint(1, 100) < 25:
        print(f"simulated error while processing record {record_id}")
        raise Exception(f"simulated error while processing record {record_id}")
    if randint(1, 100) < 25:
        return False
    return True


async def enqueue(ctx: Context, msg_id: str, previous_offset: int) -> None:
    payload = json.dumps((msg_id, previous_offset)).encode("utf-8")
    producer.produce("records_that_were_deleted", value=payload)
    producer.flush()


async def workflow(ctx: Context, record_id: str, offset: int) -> None:
    print(f"processing record {record_id} in position {offset}")
    while await ctx.run(delete_batch, record_id):
        print(f"record {record_id} still has rows to delete")
        await ctx.sleep(5)
    print(f"all rows deleted for record {record_id} in position {offset}")
    await ctx.run(enqueue, record_id, offset)


def consume(r: Resonate) -> None:
    consumer.subscribe(["records_to_be_deleted"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                record_id = json.loads(msg.value().decode("utf-8"))
                if isinstance(record_id, str):  # in case json.dumps(msg_id) was just a string
                    record_id = [record_id]
                rid = record_id[0]
                offset = msg.offset()
                # Fire-and-forget: r.run returns a handle; the Resonate server
                # tracks and executes the workflow durably.
                r.run(rid, workflow, rid, offset)
            except Exception as e:
                print(f"Error processing message: {e}")
    except KeyboardInterrupt:
        print("Shutting down consumer.")
    finally:
        consumer.close()


async def main() -> None:
    r = Resonate(
        url=os.environ.get("RESONATE_URL", "http://localhost:8001"),
        group="record-deletor-group",
    )
    r.register(delete_batch)
    r.register(enqueue)
    r.register(workflow)
    print("processor running", flush=True)
    consume(r)


if __name__ == "__main__":
    asyncio.run(main())
