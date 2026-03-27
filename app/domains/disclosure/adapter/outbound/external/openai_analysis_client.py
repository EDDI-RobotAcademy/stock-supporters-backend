import logging

from openai import AsyncOpenAI

from app.domains.disclosure.application.port.llm_analysis_port import LlmAnalysisPort
from app.infrastructure.config.settings import get_settings

logger = logging.getLogger(__name__)

ANALYSIS_MODEL = "gpt-4o-mini"
MAX_TOKENS = 4096


class OpenAIAnalysisClient(LlmAnalysisPort):
    """OpenAI Chat Completions API를 사용한 LLM 분석 어댑터"""

    def __init__(self):
        settings = get_settings()
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)

    async def analyze(self, prompt: str, system_message: str) -> str:
        """OpenAI Chat Completions API를 호출하여 분석 결과를 반환한다."""
        try:
            response = await self._client.chat.completions.create(
                model=ANALYSIS_MODEL,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=MAX_TOKENS,
                temperature=0.3,
            )
            content = response.choices[0].message.content
            logger.info("LLM 분석 완료: model=%s, tokens=%s", ANALYSIS_MODEL, response.usage.total_tokens if response.usage else "N/A")
            return content or ""
        except Exception as e:
            logger.error("OpenAI 분석 호출 실패: %s", str(e))
            raise
