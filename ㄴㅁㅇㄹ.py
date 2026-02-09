import asyncio
from temporalio import worker
from temporalio.client import Client

from workflow import LawCrawlWorkflow
from activity import crawl_law_page

async def main():
    client = await Client.connect("localhost:7233")

    w = worker.Worker(
        client,
        task_queue="LAW_CRAWL_QUEUE",
        workflows=[LawCrawlWorkflow],
        activities=[crawl_law_page],
    )

    print("Worker started: LAW_CRAWL_QUEUE")
    await w.run()

if __name__ == "__main__":
    asyncio.run(main())
