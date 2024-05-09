import asyncio
import os
import datetime
import time
from arq import create_pool
from arq.jobs import JobStatus
from arq.connections import RedisSettings
from wrapper_classes.weaviate_wrapper import WeaviateWrapper

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_SETTINGS = RedisSettings(
    host=REDIS_HOST,     
    port=REDIS_PORT,      
    password=REDIS_PASSWORD,  
    conn_timeout=300
)

WEAVIATE_HOST = os.getenv("WEAVIATE_HOST")
WEAVIATE_PORT = int(os.getenv("WEAVIATE_PORT"))
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

BATCH_SIZE = 20

async def finish_counter(job_queue):
    global start_time
    count = 0
    while True:
        job = await job_queue.get()
        
        await job.result(poll_delay=5.0)
        
        count += 1
        if count == 1:
            start_time = time.time()
        if count % 5 == 0:
            duration = time.time() - start_time
            paragraph_count = BATCH_SIZE * count
            print(f"Finished processing {paragraph_count} paragraphs [{count} batches] [{round(paragraph_count / duration * 60, 2)} paragraphs processed per min]")

        job_queue.task_done()

async def main():
    run_id = f"run_{str(datetime.datetime.now()).replace(' ', '_')}"
    pipeline_id = os.getenv("PIPELINE_ID")
    
    redis = await create_pool(REDIS_SETTINGS)
    weaviate_wrapper = WeaviateWrapper(f"http://{WEAVIATE_HOST}:{WEAVIATE_PORT}", WEAVIATE_API_KEY)
    
    job_queue = asyncio.Queue()
    counter_task = asyncio.create_task(finish_counter(job_queue))

    offset = 0
    last_id = None
    print("Starting to queue jobs")
    while True:
        query_builder = weaviate_wrapper.client.query.get("Paragraph")
        if last_id is not None:
            query_builder = query_builder.with_where({
                "path": "id",
                "operator": "GreaterThan",
                "valueString": last_id,
            })
        response = (
            query_builder
                .with_limit(BATCH_SIZE)
                .with_additional(["id"])
                .do()
        )

        if not response["data"] or not response["data"]["Get"]:  # No more data returned
            break
        
        batch = response["data"]["Get"]["Paragraph"]
        last_id = batch[-1]["_additional"]["id"] 
        id_list = [paragraph["_additional"]["id"] for paragraph in batch]
        job = await redis.enqueue_job("process_paragraphs", id_list, {"run_id": run_id, "pipeline_id": pipeline_id})
        
        job_queue.put_nowait(job)
        
        offset += BATCH_SIZE
        if (offset // BATCH_SIZE) % 5 == 0:
            print(f"Queued {offset} paragraphs [{offset // BATCH_SIZE} batches]")

        await asyncio.sleep(5)

    print("All jobs have been queued")

    await job_queue.join()
    minutes = round((time.time() - start_time) * 60, 2)
    print(f"All jobs have been completed [{minutes} mins] [{round(offset / minutes, 2)} paragraphs processed per min]")
    
    counter_task.cancel()
    
if __name__ == "__main__":
    asyncio.run(main())
