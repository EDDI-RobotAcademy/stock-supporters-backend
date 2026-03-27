REPORT_KEYWORDS = {
    "사업보고서",
    "분기보고서",
    "반기보고서",
}

EVENT_KEYWORDS_MAP = {
    "earnings": ["실적", "영업실적", "매출액", "영업이익"],
    "dividend": ["배당", "현금배당", "현물배당"],
    "fundraising": ["유상증자", "전환사채", "신주인수권", "회사채", "사채"],
    "ownership": ["대량보유", "임원ㆍ주요주주", "주식등의대량보유", "지분"],
    "major_event": ["합병", "분할", "주요사항보고", "영업양수", "영업양도"],
}


class DisclosureClassifier:

    @staticmethod
    def classify_group(report_nm: str) -> str:
        name = report_nm.strip()

        for keyword in REPORT_KEYWORDS:
            if keyword in name:
                return "report"

        for event_type, keywords in EVENT_KEYWORDS_MAP.items():
            for keyword in keywords:
                if keyword in name:
                    return "event"

        return "other"

    @staticmethod
    def classify_event_type(report_nm: str) -> str:
        name = report_nm.strip()

        for event_type, keywords in EVENT_KEYWORDS_MAP.items():
            for keyword in keywords:
                if keyword in name:
                    return event_type

        return "unknown"

    @staticmethod
    def is_core_disclosure(report_nm: str) -> bool:
        name = report_nm.strip()

        for keyword in REPORT_KEYWORDS:
            if keyword in name:
                return True

        core_events = ["유상증자", "합병", "분할", "대량보유"]
        for keyword in core_events:
            if keyword in name:
                return True

        return False
