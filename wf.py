from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activity import law_activity

@workflow.defn
class LawWorkflow:
    @workflow.run
    async def run(self):
        return await workflow.execute_activity(
            law_activity,
            start_to_close_timeout=workflow.timedelta(minutes=30)
        )
