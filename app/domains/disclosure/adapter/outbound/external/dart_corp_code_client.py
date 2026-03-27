import io
import logging
import xml.etree.ElementTree as ET
import zipfile

import httpx

from app.domains.disclosure.application.port.dart_corp_code_port import DartCorpCodePort, DartCorpInfo
from app.infrastructure.config.settings import get_settings

logger = logging.getLogger(__name__)

DART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"


class DartCorpCodeClient(DartCorpCodePort):

    async def fetch_all_corp_codes(self) -> list[DartCorpInfo]:
        settings = get_settings()

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                DART_CORP_CODE_URL,
                params={"crtfc_key": settings.open_dart_api_key},
            )
            response.raise_for_status()

        corp_list: list[DartCorpInfo] = []

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            xml_filename = zf.namelist()[0]
            with zf.open(xml_filename) as xml_file:
                tree = ET.parse(xml_file)
                root = tree.getroot()

                for item in root.findall("list"):
                    corp_code = item.findtext("corp_code", "").strip()
                    corp_name = item.findtext("corp_name", "").strip()
                    stock_code = item.findtext("stock_code", "").strip()
                    modify_date = item.findtext("modify_date", "").strip()

                    if corp_code and corp_name:
                        corp_list.append(
                            DartCorpInfo(
                                corp_code=corp_code,
                                corp_name=corp_name,
                                stock_code=stock_code if stock_code else "",
                                modify_date=modify_date,
                            )
                        )

        logger.info("DART에서 %d개 기업 코드를 수집했습니다.", len(corp_list))
        return corp_list
