import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.infrastructure.scheduler.disclosure_jobs import (
    job_incremental_collect,
    job_refresh_company_list,
    job_batch_store_documents,
    job_store_rag_chunks,
    job_cleanup_expired_data,
    job_seasonal_quarterly,
    job_seasonal_semiannual,
    job_seasonal_annual,
)

logger = logging.getLogger(__name__)


def create_disclosure_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

    # ── 수시 수집 ──

    # 매시간 정시: 증분 공시 수집
    scheduler.add_job(
        job_incremental_collect,
        trigger=CronTrigger(minute=0),
        id="incremental_collect",
        name="증분 공시 수집",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # ── 일별 운영 ──

    # 매일 02:00: 기업 리스트 갱신 (DART + KRX)
    scheduler.add_job(
        job_refresh_company_list,
        trigger=CronTrigger(hour=2, minute=0),
        id="refresh_company_list",
        name="기업 리스트 갱신",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # 매일 02:30: 공시 원문 배치 저장
    scheduler.add_job(
        job_batch_store_documents,
        trigger=CronTrigger(hour=2, minute=30),
        id="batch_store_documents",
        name="공시 원문 배치 저장",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # 매일 02:45: RAG 벡터 적재
    scheduler.add_job(
        job_store_rag_chunks,
        trigger=CronTrigger(hour=2, minute=45),
        id="store_rag_chunks",
        name="RAG 벡터 적재",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # 매일 03:00: 만료 데이터 정리
    scheduler.add_job(
        job_cleanup_expired_data,
        trigger=CronTrigger(hour=3, minute=0),
        id="cleanup_expired_data",
        name="만료 데이터 정리",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── 시즌별 보고서 집중 수집 ──

    # 분기보고서(A003): 매년 3월, 5월, 8월, 11월 15일 04:00
    # (분기 종료 후 45일 이내 제출 → 해당 월에 집중)
    scheduler.add_job(
        job_seasonal_quarterly,
        trigger=CronTrigger(month="3,5,8,11", day=15, hour=4, minute=0),
        id="seasonal_quarterly",
        name="분기보고서 시즌 수집",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 반기보고서(A002): 매년 3월, 9월 15일 04:30
    # (반기 종료 후 90일 이내 제출)
    scheduler.add_job(
        job_seasonal_semiannual,
        trigger=CronTrigger(month="3,9", day=15, hour=4, minute=30),
        id="seasonal_semiannual",
        name="반기보고서 시즌 수집",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # 사업보고서(A001): 매년 3월, 4월 1일 05:00
    # (사업연도 종료 후 90일 이내 제출 → 3~4월 집중)
    scheduler.add_job(
        job_seasonal_annual,
        trigger=CronTrigger(month="3,4", day=1, hour=5, minute=0),
        id="seasonal_annual",
        name="사업보고서 시즌 수집",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("공시 분석 스케줄러 설정 완료 (8개 작업: 수시 1, 일별 4, 시즌별 3)")
    return scheduler
