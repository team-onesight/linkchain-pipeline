import html as _html
import re
from typing import Optional

import trafilatura
from bs4 import BeautifulSoup


def extract_title_with_trafilatura(html_text: str) -> Optional[str]:
    """
    trafilatura 메타데이터를 사용하여 HTML 문서에서 title 추출

    :param html_text: HTML 텍스트
    :type html_text: str
    :return: title
    :rtype: str | None
    """
    meta = trafilatura.extract_metadata(html_text)
    if meta:
        title = getattr(meta, "title", None)
        if title:
            return title.strip()
    return None


def extract_title_with_regex(html_text: str) -> Optional[str]:
    """
    정규식을 사용하여 HTML 문서에서 title 태그 혹은 h1 태그 추출

    :param html_text: HTML 텍스트
    :type html_text: str
    :return: title
    :rtype: str | None
    """
    regex_title = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.I | re.S)
    if not regex_title:
        regex_h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, flags=re.I | re.S)
        if regex_h1:
            raw = regex_h1.group(1).strip()
            return _html.unescape(raw)
        return None
    raw = regex_title.group(1).strip()
    return _html.unescape(raw)


def extract_title_from_html(html_text: str) -> Optional[str]:
    """
    HTML 문서에서 title 추출

    :param html_text: HTML 텍스트
    :type html_text: str
    :return: title
    :rtype: str | None
    """
    title = extract_title_with_trafilatura(html_text)
    if title:
        return title
    return extract_title_with_regex(html_text)



def extract_description_with_trafilatura(html_text: str) -> Optional[str]:
    """
    trafilatura 메타데이터를 사용하여 HTML 문서에서 설명 추출

    :param html_text: HTML 텍스트
    :type html_text: str
    :return: description
    :rtype: str | None
    """
    meta = trafilatura.extract_metadata(html_text)
    if meta:
        for key in ("description", "desc", "summary", "excerpt"):
            description = getattr(meta, key, None)
            if description:
                return description.strip()
    return None


def infer_description_from_text(html_text: str, char_limit: int = 400) -> Optional[str]:
    """
    HTML 텍스트에서 설명 유추

    :param html_text: HTML 텍스트
    :type html_text: str
    :param char_limit: 최대 문자 수
    :type char_limit: int
    :return: description
    :rtype: str | None
    """
    text = trafilatura.extract(html_text)
    if not text:
        return None

    regex_desc = re.search(r"([^\n\.\?!]{20,}?[\.\?!])", text) #
    if regex_desc:
        desc = regex_desc.group(1).strip()
        if len(desc) > char_limit:
            desc = desc[:char_limit].rstrip()
        return desc

    candidate = text.strip()[:char_limit] # 최후의 수단 : 앞에서 char_limit 만큼 자르기

    return candidate if candidate else None


def extract_description_from_html(html_text: str) -> Optional[str]:
    """
    HTML 문서에서 설명 추출

    :param html_text: HTML 텍스트
    :type html_text: str
    :return: description
    :rtype: str | None
    """
    description = extract_description_with_trafilatura(html_text)
    if description:
        return description
    return infer_description_from_text(html_text) #


def extract_image_url_from_html(html_text: str) -> Optional[str]:
    """
    HTML 문자열에서 대표 image URL을 추출하는 메소드

    우선순위를 달리하여 순차적으로 og 추출 시도 후 없으면 None 반환
    1. trafilatura.extract_metadata().image
    2. og:image (bs4 + lxml)

    :param html_text: HTML 텍스트
    :return: image URL | None
    """ # noqa: E501
    if not html_text:
        return None

    # 우선순위: trafilatura 사용
    try:
        metadata = trafilatura.extract_metadata(html_text)
        if metadata and metadata.image:
            return metadata.image.strip()
    except Exception: # noqa: S110
        pass

    # 차선: bs4 사용
    soup = BeautifulSoup(html_text, "lxml")

    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        return og_image["content"].strip()

    return None


def extract_records_from_html(html_text: str) -> tuple:
    """
    directory에서 HTML 파일을 추출하여 튜플로 반환

    :param dir_path: path
    :type dir_path: str
    :return: html 파일에서 추출한 title, description
    :rtype: tuple
    """
    title = extract_title_from_html(html_text)
    description = extract_description_from_html(html_text) #
    image_url = extract_image_url_from_html(html_text)

    return title, description, image_url
