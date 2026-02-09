from temporalio import workflow

# Activity import는 반드시 unsafe 블록 안에서
with workflow.unsafe.imports_passed_through():
    from activity import crawl_law_page

@workflow.defn
class LawCrawlWorkflow:
    @workflow.run
    async def run(self):
        """
        🔥 입력값 ZERO
        → 내부에서 페이지 범위 고정 사용
        """

        START_PAGE = 50
        END_PAGE = 66

        tasks = []
        for p in range(START_PAGE, END_PAGE + 1):
            tasks.append(
                workflow.execute_activity(
                    crawl_law_page,
                    p,
                    start_to_close_timeout=workflow.timedelta(minutes=30)
                )
            )

        # 각 페이지별 저장 경로 리스트 반환
        results = await workflow.gather(*tasks)
        return results
