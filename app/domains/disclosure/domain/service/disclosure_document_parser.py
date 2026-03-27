import re
from typing import Any, Optional


class DisclosureDocumentParser:
    """공시 원문 텍스트를 구조화된 데이터로 파싱하는 도메인 서비스.

    순수 Python만 사용하며, 외부 라이브러리 의존성이 없다.
    """

    # 공시 문서에서 자주 등장하는 섹션 구분 패턴
    _SECTION_PATTERNS = [
        r"^[IVX]+\.\s+(.+)$",               # I. 회사의 개요
        r"^제?\s*\d+\s*[조항편장절]\s*[.\s]*(.+)$",  # 제1조 목적
        r"^\d+\.\s+(.+)$",                   # 1. 회사의 개요
        r"^[가-힣]\.\s+(.+)$",               # 가. 회사의 개요
        r"^【(.+)】$",                        # 【회사의 개요】
        r"^\[(.+)\]$",                        # [회사의 개요]
    ]

    _SUMMARY_MAX_LENGTH = 500

    def parse(self, raw_text: Optional[str]) -> dict[str, Any]:
        """원문 텍스트를 파싱하여 구조화된 JSON 데이터를 반환한다.

        Args:
            raw_text: 공시 원문 텍스트

        Returns:
            파싱된 구조화 데이터 (title, sections, tables, metadata)
        """
        if not raw_text or not raw_text.strip():
            return {
                "title": "",
                "sections": [],
                "tables": [],
                "metadata": {"total_length": 0, "section_count": 0},
            }

        lines = raw_text.strip().splitlines()
        title = self._extract_title(lines)
        sections = self._extract_sections(lines)
        tables = self._extract_tables(lines)

        return {
            "title": title,
            "sections": sections,
            "tables": tables,
            "metadata": {
                "total_length": len(raw_text),
                "section_count": len(sections),
                "table_count": len(tables),
            },
        }

    def generate_summary(self, raw_text: Optional[str]) -> str:
        """원문 텍스트에서 요약 텍스트를 생성한다.

        첫 번째 의미 있는 섹션의 내용을 기반으로 요약을 생성한다.

        Args:
            raw_text: 공시 원문 텍스트

        Returns:
            요약 텍스트 (최대 500자)
        """
        if not raw_text or not raw_text.strip():
            return ""

        lines = raw_text.strip().splitlines()

        # 빈 줄과 짧은 줄을 제외한 의미 있는 텍스트를 수집
        meaningful_lines = []
        for line in lines:
            stripped = line.strip()
            if len(stripped) > 5:
                meaningful_lines.append(stripped)

        if not meaningful_lines:
            return ""

        summary = " ".join(meaningful_lines)
        if len(summary) > self._SUMMARY_MAX_LENGTH:
            summary = summary[: self._SUMMARY_MAX_LENGTH] + "..."

        return summary

    def _extract_title(self, lines: list[str]) -> str:
        """문서의 제목을 추출한다."""
        for line in lines[:20]:
            stripped = line.strip()
            if not stripped:
                continue
            # 제목은 보통 문서 상단의 짧은 줄
            if 2 < len(stripped) < 200:
                return stripped
        return ""

    def _extract_sections(self, lines: list[str]) -> list[dict[str, str]]:
        """문서에서 섹션을 추출한다."""
        sections: list[dict[str, str]] = []
        current_heading = ""
        current_content_lines: list[str] = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            heading = self._match_section_heading(stripped)
            if heading is not None:
                # 이전 섹션 저장
                if current_heading:
                    sections.append({
                        "heading": current_heading,
                        "content": "\n".join(current_content_lines).strip(),
                    })
                current_heading = heading
                current_content_lines = []
            else:
                current_content_lines.append(stripped)

        # 마지막 섹션 저장
        if current_heading:
            sections.append({
                "heading": current_heading,
                "content": "\n".join(current_content_lines).strip(),
            })

        return sections

    def _match_section_heading(self, line: str) -> Optional[str]:
        """줄이 섹션 제목 패턴에 매칭되는지 확인한다."""
        for pattern in self._SECTION_PATTERNS:
            match = re.match(pattern, line)
            if match:
                return line
        return None

    def _extract_tables(self, lines: list[str]) -> list[dict[str, Any]]:
        """텍스트에서 테이블 구조를 추출한다.

        구분자 기반(탭, |, 연속 공백)으로 테이블 행을 감지한다.
        """
        tables: list[dict[str, Any]] = []
        current_table_rows: list[list[str]] = []
        in_table = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                if in_table and current_table_rows:
                    tables.append(self._build_table(current_table_rows))
                    current_table_rows = []
                    in_table = False
                continue

            cells = self._try_parse_table_row(stripped)
            if cells is not None and len(cells) >= 2:
                current_table_rows.append(cells)
                in_table = True
            else:
                if in_table and current_table_rows:
                    tables.append(self._build_table(current_table_rows))
                    current_table_rows = []
                    in_table = False

        # 마지막 테이블
        if current_table_rows:
            tables.append(self._build_table(current_table_rows))

        return tables

    @staticmethod
    def _try_parse_table_row(line: str) -> Optional[list[str]]:
        """줄을 테이블 행으로 파싱을 시도한다."""
        # 탭 구분자
        if "\t" in line:
            cells = [cell.strip() for cell in line.split("\t")]
            if len(cells) >= 2:
                return cells

        # 파이프 구분자
        if "|" in line:
            cells = [cell.strip() for cell in line.split("|")]
            cells = [c for c in cells if c]  # 빈 셀 제거
            if len(cells) >= 2:
                return cells

        return None

    @staticmethod
    def _build_table(rows: list[list[str]]) -> dict[str, Any]:
        """테이블 행 목록에서 테이블 딕셔너리를 구성한다."""
        if not rows:
            return {"headers": [], "rows": []}

        return {
            "headers": rows[0],
            "rows": rows[1:] if len(rows) > 1 else [],
        }
