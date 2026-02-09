from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activity import crawl_law_page

@workflow.defn
class LawCrawlWorkflow:
    @workflow.run
    async def run(self, start_page: int, end_page: int, workers: int, browser_count: int):
        tasks = []

        for p in range(start_page, end_page + 1):
            tasks.append(
                workflow.execute_activity(
                    crawl_law_page,
                    p, workers, browser_count,
                    start_to_close_timeout=workflow.timedelta(minutes=30)
                )
            )

        results = await workflow.gather(*tasks)
        return results   # → 각 페이지별 JSON 경로 리스트
