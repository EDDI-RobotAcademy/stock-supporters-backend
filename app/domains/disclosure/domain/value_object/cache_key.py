class CacheKey:
    """캐시 키 생성을 위한 Value Object (순수 Python)"""

    PREFIX = "disclosure:analysis"

    @staticmethod
    def generate(corp_code: str, analysis_type: str) -> str:
        """캐시 키를 생성한다.

        Args:
            corp_code: 기업 코드 (비어있으면 안 됨)
            analysis_type: 분석 유형 (비어있으면 안 됨)

        Returns:
            "disclosure:analysis:{corp_code}:{analysis_type}" 형식의 캐시 키

        Raises:
            ValueError: corp_code 또는 analysis_type이 비어있는 경우
        """
        if not corp_code or not corp_code.strip():
            raise ValueError("corp_code는 비어있을 수 없습니다.")
        if not analysis_type or not analysis_type.strip():
            raise ValueError("analysis_type은 비어있을 수 없습니다.")

        return f"{CacheKey.PREFIX}:{corp_code.strip()}:{analysis_type.strip()}"
