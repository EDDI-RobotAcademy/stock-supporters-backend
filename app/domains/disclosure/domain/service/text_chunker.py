import hashlib
import re


class TextChunker:
    """장문 텍스트를 청크 단위로 분할하는 도메인 서비스.

    순수 Python만 사용한다 (hashlib, re).
    """

    DEFAULT_CHUNK_SIZE = 1000
    DEFAULT_OVERLAP = 200

    SENTENCE_DELIMITERS = re.compile(r"(?<=[.!?\n])\s+")
    SECTION_TITLE_PATTERN = re.compile(
        r"^(?:제\s*\d+\s*[조장절편]|[IVX]+\.\s*|\d+\.\s+|【.+?】)(.+)",
        re.MULTILINE,
    )

    def chunk_text(
        self,
        text: str,
        chunk_size: int | None = None,
        overlap: int | None = None,
    ) -> list[dict]:
        """텍스트를 청크 단위로 분할하여 리스트를 반환한다.

        Returns:
            list of dict with keys: chunk_index, chunk_text, section_title, chunk_hash
        """
        if not text or not text.strip():
            return []

        chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        overlap = overlap or self.DEFAULT_OVERLAP

        sentences = self._split_into_sentences(text)
        chunks = self._merge_sentences_into_chunks(sentences, chunk_size, overlap)

        results = []
        for idx, chunk_text in enumerate(chunks):
            section_title = self._detect_section_title(chunk_text)
            chunk_hash = self._generate_hash(chunk_text)
            results.append(
                {
                    "chunk_index": idx,
                    "chunk_text": chunk_text,
                    "section_title": section_title,
                    "chunk_hash": chunk_hash,
                }
            )

        return results

    def _split_into_sentences(self, text: str) -> list[str]:
        """문장 경계(마침표, 느낌표, 물음표, 개행)에서 텍스트를 분할한다."""
        parts = self.SENTENCE_DELIMITERS.split(text)
        return [p.strip() for p in parts if p.strip()]

    def _merge_sentences_into_chunks(
        self,
        sentences: list[str],
        chunk_size: int,
        overlap: int,
    ) -> list[str]:
        """문장들을 chunk_size에 맞게 병합한다. overlap 만큼 겹치도록 한다."""
        if not sentences:
            return []

        chunks: list[str] = []
        current_sentences: list[str] = []
        current_length = 0

        for sentence in sentences:
            sentence_length = len(sentence)

            if current_length + sentence_length > chunk_size and current_sentences:
                chunk_text = " ".join(current_sentences)
                chunks.append(chunk_text)

                # overlap 만큼 뒤에서부터 문장을 유지
                overlap_sentences: list[str] = []
                overlap_length = 0
                for s in reversed(current_sentences):
                    if overlap_length + len(s) > overlap:
                        break
                    overlap_sentences.insert(0, s)
                    overlap_length += len(s)

                current_sentences = overlap_sentences
                current_length = overlap_length

            current_sentences.append(sentence)
            current_length += sentence_length

        if current_sentences:
            chunks.append(" ".join(current_sentences))

        return chunks

    def _detect_section_title(self, chunk_text: str) -> str | None:
        """청크 텍스트의 첫 줄에서 섹션 제목 패턴을 감지한다."""
        first_line = chunk_text.split("\n")[0].strip()
        match = self.SECTION_TITLE_PATTERN.match(first_line)
        if match:
            return first_line[:255]

        # 짧은 첫 줄은 제목일 가능성이 있음
        if len(first_line) < 50 and first_line and not first_line.endswith((".", "다", "요")):
            return first_line[:255]

        return None

    @staticmethod
    def _generate_hash(text: str) -> str:
        """텍스트의 SHA-256 해시를 생성한다. 동일 텍스트는 동일 해시를 반환."""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
