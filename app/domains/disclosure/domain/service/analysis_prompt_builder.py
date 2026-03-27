"""공시 분석을 위한 LLM 프롬프트 빌더 (순수 Python 도메인 서비스)"""


class AnalysisPromptBuilder:

    @staticmethod
    def _format_disclosures(disclosures: list) -> str:
        """공시 목록을 텍스트로 포맷팅한다."""
        if not disclosures:
            return "공시 데이터가 없습니다."

        lines = []
        for d in disclosures:
            rcept_dt = str(d.rcept_dt) if hasattr(d, "rcept_dt") else "N/A"
            report_nm = d.report_nm if hasattr(d, "report_nm") else "N/A"
            group = d.disclosure_group if hasattr(d, "disclosure_group") else "N/A"
            lines.append(f"- [{rcept_dt}] {report_nm} (분류: {group})")
        return "\n".join(lines)

    @staticmethod
    def _format_rag_contexts(rag_contexts: list) -> str:
        """RAG 검색 결과를 텍스트로 포맷팅한다."""
        if not rag_contexts:
            return "추가 근거 자료가 없습니다."

        lines = []
        for i, ctx in enumerate(rag_contexts, 1):
            report_nm = ctx.report_nm if hasattr(ctx, "report_nm") else "N/A"
            section = ctx.section_title if hasattr(ctx, "section_title") else "N/A"
            text = ctx.chunk_text if hasattr(ctx, "chunk_text") else ""
            lines.append(f"[근거 {i}] {report_nm} - {section}\n{text}")
        return "\n\n".join(lines)

    @classmethod
    def build_flow_analysis_prompt(
        cls, disclosures: list, rag_contexts: list
    ) -> tuple:
        """공시 흐름 시계열 분석 프롬프트를 생성한다.

        Returns:
            tuple[str, str]: (사용자 프롬프트, 시스템 메시지)
        """
        disclosure_text = cls._format_disclosures(disclosures)
        rag_text = cls._format_rag_contexts(rag_contexts)

        system_message = (
            "당신은 한국 주식 시장의 공시 분석 전문가입니다. "
            "공시 데이터를 시계열로 분석하여 기업의 흐름과 변화를 파악합니다. "
            "반드시 한국어로 답변하고, JSON 형식으로 출력하세요."
        )

        prompt = f"""다음 기업의 공시 목록을 시계열로 분석하여 주요 흐름과 변화를 파악해주세요.

## 공시 목록
{disclosure_text}

## 참고 자료 (RAG 검색 결과)
{rag_text}

## 출력 형식 (JSON)
다음 JSON 형식으로 출력해주세요:
{{
    "timeline_summary": "시계열 흐름 요약",
    "key_events": [
        {{
            "date": "날짜",
            "event": "이벤트 설명",
            "significance": "중요도 (high/medium/low)",
            "detail": "상세 분석"
        }}
    ],
    "trend_analysis": "전체적인 트렌드 분석",
    "risk_factors": ["위험 요소 1", "위험 요소 2"],
    "positive_signals": ["긍정 신호 1", "긍정 신호 2"]
}}"""

        return prompt, system_message

    @classmethod
    def build_signal_analysis_prompt(
        cls, disclosures: list, rag_contexts: list
    ) -> tuple:
        """공시 기반 투자 신호 분석 프롬프트를 생성한다.

        Returns:
            tuple[str, str]: (사용자 프롬프트, 시스템 메시지)
        """
        disclosure_text = cls._format_disclosures(disclosures)
        rag_text = cls._format_rag_contexts(rag_contexts)

        system_message = (
            "당신은 한국 주식 시장의 투자 신호 분석 전문가입니다. "
            "공시 데이터를 기반으로 투자에 유의미한 신호를 분석합니다. "
            "반드시 한국어로 답변하고, JSON 형식으로 출력하세요."
        )

        prompt = f"""다음 기업의 공시 데이터를 분석하여 투자 신호를 도출해주세요.

## 공시 목록
{disclosure_text}

## 참고 자료 (RAG 검색 결과)
{rag_text}

## 출력 형식 (JSON)
다음 JSON 형식으로 출력해주세요:
{{
    "overall_signal": "종합 신호 (bullish/bearish/neutral)",
    "confidence": "신뢰도 (0.0 ~ 1.0)",
    "signals": [
        {{
            "type": "신호 유형 (earnings/dividend/fundraising/ownership/major_event)",
            "direction": "방향 (positive/negative/neutral)",
            "strength": "강도 (strong/moderate/weak)",
            "description": "신호 설명",
            "based_on": "근거 공시"
        }}
    ],
    "investment_summary": "투자 관점 요약",
    "cautions": ["유의사항 1", "유의사항 2"]
}}"""

        return prompt, system_message

    @classmethod
    def build_full_analysis_prompt(
        cls, disclosures: list, rag_contexts: list
    ) -> tuple:
        """종합 공시 분석 프롬프트를 생성한다.

        Returns:
            tuple[str, str]: (사용자 프롬프트, 시스템 메시지)
        """
        disclosure_text = cls._format_disclosures(disclosures)
        rag_text = cls._format_rag_contexts(rag_contexts)

        system_message = (
            "당신은 한국 주식 시장의 공시 분석 전문가입니다. "
            "공시 데이터를 종합적으로 분석하여 시계열 흐름, 투자 신호, "
            "리스크 요인을 통합 평가합니다. "
            "반드시 한국어로 답변하고, JSON 형식으로 출력하세요."
        )

        prompt = f"""다음 기업의 공시 데이터를 종합적으로 분석해주세요.

## 공시 목록
{disclosure_text}

## 참고 자료 (RAG 검색 결과)
{rag_text}

## 출력 형식 (JSON)
다음 JSON 형식으로 출력해주세요:
{{
    "company_overview": "기업 공시 현황 요약",
    "timeline_summary": "시계열 흐름 요약",
    "key_events": [
        {{
            "date": "날짜",
            "event": "이벤트 설명",
            "significance": "중요도 (high/medium/low)",
            "detail": "상세 분석"
        }}
    ],
    "overall_signal": "종합 투자 신호 (bullish/bearish/neutral)",
    "confidence": "신뢰도 (0.0 ~ 1.0)",
    "signals": [
        {{
            "type": "신호 유형",
            "direction": "방향 (positive/negative/neutral)",
            "strength": "강도 (strong/moderate/weak)",
            "description": "신호 설명"
        }}
    ],
    "risk_factors": ["위험 요소 1", "위험 요소 2"],
    "positive_signals": ["긍정 신호 1", "긍정 신호 2"],
    "investment_summary": "투자 관점 종합 요약",
    "disclaimer": "본 분석은 공시 데이터 기반의 참고 자료이며, 투자 판단의 책임은 투자자에게 있습니다."
}}"""

        return prompt, system_message
