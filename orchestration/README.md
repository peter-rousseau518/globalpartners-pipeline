# Orchestration

| File | What it is |
| --- | --- |
| `state_machine.asl.json` | Step Functions workflow `gp-daily-pipeline` (deployed): start SQL Server → wait until available → Bronze → Silver → Gold model → Gold metrics → stop SQL Server → email. Each Glue step retries twice (5 and 10 minutes apart); any failure emails the error and stops SQL Server. |
| `state_machine_role_policy.json` | Least-privilege permissions for the workflow's role: run the four Glue jobs, start/stop/describe the RDS instance, publish to the alert topic. |
| `schedule.json` | EventBridge Scheduler definition for a daily 6:00 AM US Eastern run. **Designed, not deployed** in the demo account to control cost; runs are started on demand. |

## Run on demand
Step Functions → State machines → `gp-daily-pipeline` → **Start execution** (input `{}`).

## Recover from a failure
1. Read the failure email (it includes the failed step, error and cause).
2. Fix the cause.
3. Open the failed execution → **Redrive**. It resumes from the failed step; finished steps are not re-run.

Every job is idempotent: Bronze overwrites only that day's partition and Silver/Gold rebuild their tables, so re-running a day never duplicates data.

## Turn on the daily schedule
Replace `ACCOUNT_ID` in `schedule.json`, create a role that EventBridge Scheduler can assume with `states:StartExecution` on the state machine, then:
`aws scheduler create-schedule --cli-input-json file://orchestration/schedule.json`
