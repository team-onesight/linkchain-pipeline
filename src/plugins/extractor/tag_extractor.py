import re
import pandas as pd
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
import spacy
import MeCab

#####################
UNIT_TOKENS = {
    "cm", "mm", "m", "km",
    "kg", "g", "ton",
    "sec", "min", "hr",
    "mb", "gb", "tb"
}

STOP_WORDS_KO = {
    "것", "수", "등", "더", "중", "바로", "이것", "저것",
    "때", "곳", "의미", "내용", "부분", "관계", "이유",
    "방법", "문제", "사항", "현상", "대해", "관련",
    "올해", "이번", "지난", "매우", "아주", "정말"
}

STOP_WORDS_EN = {
    "thing", "things", "time", "case", "way", "year",
    "day", "week", "people", "issue", "problem"
    "png", "jpg", "jpeg", "gif", "bmp"
}

TECH_KEYWORDS = {
    "redis", "mysql", "postgresql", "kafka", "spark",
    "hadoop", "airflow", "docker", "kubernetes"
}

#####################
_MECAB = MeCab.Tagger()


_SPACY_NLP = spacy.load("en_core_web_sm")

embedding_model = SentenceTransformer(
    "/opt/airflow/model"
)
kw_model = KeyBERT(model=embedding_model)
#####################

def detect_language(text: str) -> str:
    """
    언어 감지
    :param text: 입력 텍스트
    :type text: str
    :return: 언어 코드 ("ko", "en", "other")
    :rtype: str
    """
    if re.search(r"[ㄱ-ㅎㅏ-ㅣ가-힣]", text):
        return "ko"
    if re.search(r"[A-Za-z]", text):
        return "en"
    return "other"


def extract_korean_nouns(text: str) -> list[str]:
    """
    extract_korean_nouns의 Docstring
    
    :param text: 입력 텍스트
    :type text: str
    :return: 태그리스트
    :rtype: list[str]
    """
    if not text:
        return []

    nouns = set()
    parsed = _MECAB.parse(text)

    for line in parsed.splitlines():
        if line == "EOS":
            break

        try:
            surface, features = line.split("\t")
        except ValueError:
            continue

        pos = features.split(",")[0]
        s = surface.strip()
        sl = s.lower()

        if pos in ("NNG", "NNP", "NNB"):
            if (
                len(s) >= 2
                and s not in STOP_WORDS_KO
            ):
                nouns.add(s)

        elif pos == "SL":
            if (
                len(sl) >= 3 and
                sl not in UNIT_TOKENS and
                sl not in STOP_WORDS_EN and
                sl.isalpha()
            ):
                nouns.add(sl)

    return list(nouns)


def extract_english_nouns(text: str) -> list[str]:
    """
    영어 명사(token 단위) 추출
    :param text: 입력 텍스트
    :type text: str
    :return: 명사 리스트
    :rtype: list[str]
    """
    if not text:
        return []

    doc = _SPACY_NLP(text)
    results = []
    seen = set()

    for token in doc:
        # 명사 계열만
        if token.pos_ not in {"NOUN", "PROPN"}:
            continue

        lemma = token.lemma_.lower()

        # 기본 필터
        if not lemma.isalpha():
            continue
        if len(lemma) < 3:
            continue
        if lemma in STOP_WORDS_EN:
            continue
        if lemma in UNIT_TOKENS:
            continue
        if token.like_num:
            continue

        if lemma not in seen:
            seen.add(lemma)
            results.append(lemma)

    # 기술 키워드는 우선순위 부여
    for kw in TECH_KEYWORDS:
        if kw in text.lower() and kw not in seen:
            results.insert(0, kw)
            seen.add(kw)

    return results


def generate_multilang_tags(
    title_desc_df: pd.DataFrame,
    top_n_phrases: int = 8,
    max_tags: int = 5
) -> pd.DataFrame:
    """
    title_desc_df에서 다국어 태그 생성
    
    :param title_desc_df: DataFrame with {link_id, title, description}
    :type title_desc_df: pd.DataFrame
    :param top_n_phrases: 키워드 추출 시도 개수
    :type top_n_phrases: int
    :param max_tags: 태그 최대 개수
    :type max_tags: int
    :return: DataFrame with { link_id, title, description, tags }
    :rtype: pd.DataFrame
    """
    if title_desc_df.empty:
        return pd.DataFrame()

    df = title_desc_df.copy() # mutable 하기 때문에

    texts = (
        df["title"].fillna("") + " " +
        df["description"].fillna("")
    ).tolist()

    keywords_list = kw_model.extract_keywords(
        texts,
        keyphrase_ngram_range=(1, 3),
        stop_words=None,
        top_n=top_n_phrases
    )

    tags_col = []

    for keywords in keywords_list:
        tag_set = []
        seen = set()
        sorted_keywords = sorted(
            keywords,
            key=lambda x: x[1],
            reverse=True
        )
        for phrase, _score in sorted_keywords:

            lang = detect_language(phrase)

            if lang == "ko":
                nouns = extract_korean_nouns(phrase)
            elif lang == "en":
                nouns = extract_english_nouns(phrase)
            else:
                nouns = []
                nouns += extract_korean_nouns(phrase)
                nouns += extract_english_nouns(phrase)

            # Tag 개수 채웠으면 break
            for noun in nouns:
                if noun not in seen and len(tag_set) < max_tags:
                    seen.add(noun)
                    tag_set.append(noun)
            if len(tag_set) >= max_tags:
                break

        tags_col.append(tag_set)

    df["tags"] = tags_col
    return df