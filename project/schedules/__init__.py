from dagster import ScheduleDefinition
from ..jobs import update_update_job

update_sql_sche = ScheduleDefinition(
    job=update_update_job,
    cron_schedule="10 16 * * 1",
)