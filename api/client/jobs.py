# jobs.py
from typing import Optional, Dict, Any, List
import io
import os
import time
import uuid
import json as _json
import logging
import requests
import re
from pypdf import PdfReader
from openai import OpenAI

logger = logging.getLogger("course-jobs")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

COURSE_REQUEST_TIME_BUDGET_S = 60.0
COURSE_REWRITE_TIMEOUT_S = 15.0
COURSE_REWRITE_MAX = 6


def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Given raw PDF bytes, extract all text as a single string.
    Very close to what the original /course-create logic does.
    """
    if not pdf_bytes:
        return ""

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except Exception:
        return ""

    texts: List[str] = []
    for i, page in enumerate(getattr(reader, "pages", [])):
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            # include page markers (nice for debugging, optional)
            texts.append(f"[Page {i+1}]\n{t.strip()}")

    return "\n\n".join(texts).strip()


def _chunk_text(text: str, max_chars: int = 3000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks so that each chunk has at most max_chars
    characters, and consecutive chunks overlap by `overlap` characters.
    """
    if not text:
        return []

    if max_chars <= 0:
        return [text]

    chunks: List[str] = []
    n = len(text)
    start = 0

    while start < n:
        end = min(start + max_chars, n)
        chunk = text[start:end]
        chunks.append(chunk)

        if end >= n:
            break

        start = max(0, end - overlap)

    return chunks


def _sanitize_json_response(raw: str) -> str:
    """
    Take the raw LLM response and strip code fences / extra text,
    returning a string that should be a valid JSON object.
    """
    if not raw:
        return "{}"

    text = raw.strip()

    # Remove ```json / ``` fences
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()

    start = text.find("{")
    end = text.rfind("}")

    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    return text.strip()


def _count_words(text: str) -> int:
    if not text:
        return 0
    return len(text.strip().split())


def _trim_to_word_limit(text: str, max_words: int) -> str:
    if not text or max_words <= 0:
        return ""

    words = text.strip().split()
    if len(words) <= max_words:
        return text.strip()

    return " ".join(words[:max_words]).strip()


def process_course_job(
    pdf_bytes: Optional[bytes],
    prompt: str,
    items_per_module: int,
    pdf_url: Optional[str],
    cohort_passwords: Dict[str, str],
    trace_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Heavy job: PDF extraction, chunking, LLM calls, post-processing.
    This mirrors the original /course-create route logic,
    but returns plain data and uses logger instead of Flask.
    """

    if trace_id is None:
        trace_id = str(uuid.uuid4())

    # 0) Make sure we actually have PDF bytes. If only pdf_url was provided, download it here.
    if not pdf_bytes and pdf_url:
        try:
            logger.info(f"[course-job] trace_id={trace_id} downloading pdf_url={pdf_url}")
            resp = requests.get(pdf_url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
            logger.info(
                f"[course-job] trace_id={trace_id} downloaded size_bytes={len(pdf_bytes)} "
                f"status_code={resp.status_code}"
            )
        except Exception as e:
            logger.exception(f"[course-job] trace_id={trace_id} download error: {e}")
            raise RuntimeError(f"Failed to download PDF from url: {e}") from e

    if not pdf_bytes:
        raise RuntimeError("Failed to obtain PDF bytes")

    request_start_time = time.time()

    # ---- Extract and chunk text (same idea as original route) ----
    try:
        try:
            _reader = PdfReader(io.BytesIO(pdf_bytes))
            page_count = len(_reader.pages)
        except Exception:
            page_count = None

        extracted_text = _extract_text_from_pdf_bytes(pdf_bytes)
        logger.info(
            f"[course-job] trace_id={trace_id} extracted_text_len={len(extracted_text)} "
            f"page_count={page_count}"
        )
    except Exception as e:
        logger.error(
            f"[course-job] trace_id={trace_id} extraction error: {str(e)}",
            exc_info=True,
        )
        raise RuntimeError("Unable to extract text from PDF") from e

    if not extracted_text:
        raise RuntimeError("Unable to extract text from PDF")

    chunks = _chunk_text(extracted_text, max_chars=3000, overlap=200)
    if chunks:
        first_len = len(chunks[0])
        last_len = len(chunks[-1])
    else:
        first_len = 0
        last_len = 0

    logger.info(
        f"[course-job] trace_id={trace_id} chunks_count={len(chunks)} "
        f"first_chunk_len={first_len} last_chunk_len={last_len}"
    )

    max_chunks = 4
    selected_chunks = chunks[:max_chunks]
    corpus = "\n\n".join([f"[Chunk {i+1}]\n" + c for i, c in enumerate(selected_chunks)])
    if len(corpus) > 20000:
        corpus = corpus[:20000]
    logger.info(
        f"[course-job] trace_id={trace_id} selected_chunks={len(selected_chunks)} "
        f"corpus_len={len(corpus)}"
    )

    if not OPENAI_API_KEY:
        logger.error(f"[course-job] trace_id={trace_id} missing OPENAI_API_KEY")
        raise RuntimeError("OPENAI_API_KEY not configured")

    client = OpenAI(api_key=OPENAI_API_KEY)

    system_instructions = (
        "You are an expert instructional designer. Given a source corpus and a user prompt, "
        "produce a high-quality CourseStruct JSON. Each module must include a contentList with exactly "
        f"{items_per_module} items by default unless otherwise specified. Return strictly valid JSON only."
    )

    user_instructions = {
        "task": "Create a course structure JSON (CourseStruct)",
        "prompt": prompt,
        "items_per_module": items_per_module,
        "schema": {
            "title": "string",
            "description": "string",
            "modules": [
                {
                    "title": "string",
                    "pdfUrl": "string?",
                    "contentList": [
                        {
                            "title": "string",
                            "content": "string",
                            "pdfUrl": "string?",
                            "moduleTitle": "string?",
                            "mediaUrl": "string?",
                            "mediaType": "image|pdf?",
                        }
                    ],
                }
            ],
        },
        "source_corpus": corpus[:15000],
        "notes": [
            "Use the source_corpus to ground the content.",
            "Ensure modules are coherent and sequential.",
            "Use concise, skimmable, beginner-friendly language that assumes no prior knowledge.",
            "For each contentList item, write a clear mini-lesson of 300-500 words that explains the topic at hand in simple terms, includes definitions, everyday analogies, and a short step-by-step where relevant.",
            "Set each module.contentList length to items_per_module by default.",
            "If a pdf_url was provided, include it as module.pdfUrl and contentList[i].pdfUrl where relevant.",
            "Do not include any text outside of the JSON object.",
        ],
    }

    model_name = os.getenv("OPENAI_MODEL", "gpt-4o")
    initial_timeout = float(os.getenv("COURSE_INITIAL_TIMEOUT_S", "60"))
    
    logger.info(
        f"[course-job] trace_id={trace_id} invoking LLM model={model_name} "
        f"items_per_module={items_per_module} initial_timeout_s={initial_timeout}"
    )

    completion = client.chat.completions.create(
        model=model_name,
        temperature=0.4,
        messages=[
            {"role": "system", "content": system_instructions},
            {"role": "user", "content": _json.dumps(user_instructions)},
        ],
        timeout=initial_timeout,
    )

    content = (
        completion.choices[0].message.content if completion and completion.choices else ""
    )
    logger.info(
        f"[course-job] trace_id={trace_id} llm_response_len={len(content) if content else 0}"
    )
    if not content:
        logger.error(f"[course-job] trace_id={trace_id} empty LLM response")
        raise RuntimeError("LLM returned empty response")

    # Parse JSON
    try:
        raw_json = _sanitize_json_response(content)
        course_obj: Dict[str, Any] = _json.loads(raw_json)
        modules_count = (
            len(course_obj.get("modules", []) or [])
            if isinstance(course_obj, dict)
            else 0
        )
        logger.info(
            f"[course-job] trace_id={trace_id} parsed_json keys="
            f"{list(course_obj.keys()) if isinstance(course_obj, dict) else 'n/a'} "
            f"modules_count={modules_count}"
        )
    except Exception as e:
        logger.error(
            f"[course-job] trace_id={trace_id} json_parse_error: {str(e)}",
            exc_info=True,
        )
        raise RuntimeError(f"Failed to parse LLM JSON: {e}") from e

    # ---- Post-process: enforce items_per_module + 300–500 words ----
    try:
        modules = course_obj.get("modules", [])
        if isinstance(modules, list):
            for m in modules:
                clist = m.get("contentList")
                if isinstance(clist, list):
                    if len(clist) > items_per_module:
                        m["contentList"] = clist[:items_per_module]
                        logger.info(
                            f"[course-job] trace_id={trace_id} truncated contentList "
                            f"from {len(clist)} to {items_per_module}"
                        )
                    elif len(clist) < items_per_module and len(clist) > 0:
                        last_item = clist[-1]
                        while len(m["contentList"]) < items_per_module:
                            m["contentList"].append(last_item)
                        logger.info(
                            f"[course-job] trace_id={trace_id} padded contentList "
                            f"to {items_per_module}"
                        )

                    rewrites_used = 0
                    for item in m["contentList"]:
                        try:
                            content_text = (
                                item.get("content", "") if isinstance(item, dict) else ""
                            )
                            word_count = _count_words(content_text)
                            if word_count == 0:
                                continue

                            if word_count < 300:
                                time_spent = time.time() - request_start_time
                                time_left = COURSE_REQUEST_TIME_BUDGET_S - time_spent
                                if (
                                    COURSE_REWRITE_MAX > 0
                                    and rewrites_used < COURSE_REWRITE_MAX
                                    and time_left > 2.0
                                ):
                                    rewrite_prompt = {
                                        "instruction": (
                                            "Rewrite this explanation to be beginner-friendly and "
                                            "300-500 words. Use simple language, define terms, and "
                                            "include a short step-by-step and a relatable analogy. "
                                            "Return ONLY the rewritten text."
                                        ),
                                        "topic": item.get("title")
                                        or m.get("title")
                                        or "",
                                        "original_text": content_text,
                                        "target_word_range": "300-500",
                                    }
                                    try:
                                        per_call_timeout = max(
                                            3.0,
                                            min(
                                                COURSE_REWRITE_TIMEOUT_S,
                                                time_left - 1.0,
                                            ),
                                        )
                                        expansion = client.chat.completions.create(
                                            model=model_name,
                                            temperature=0.3,
                                            messages=[
                                                {
                                                    "role": "system",
                                                    "content": "You improve course content for beginners.",
                                                },
                                                {
                                                    "role": "user",
                                                    "content": _json.dumps(
                                                        rewrite_prompt
                                                    ),
                                                },
                                            ],
                                            timeout=per_call_timeout,
                                        )
                                        new_text = (
                                            expansion.choices[0].message.content or ""
                                        ).strip()
                                        new_text = _trim_to_word_limit(new_text, 520)
                                        item["content"] = new_text
                                        rewrites_used += 1
                                    except Exception:
                                        # if rewrite fails, keep original content
                                        pass
                            elif word_count > 500:
                                item["content"] = _trim_to_word_limit(
                                    content_text, 520
                                )
                        except Exception:
                            continue

        # Attach pdfUrl/media fields if pdf_url was provided
        if pdf_url:
            for m in course_obj.get("modules", []) or []:
                if "pdfUrl" not in m or not m["pdfUrl"]:
                    m["pdfUrl"] = pdf_url
                if isinstance(m.get("contentList"), list):
                    for item in m["contentList"]:
                        if "pdfUrl" not in item or not item["pdfUrl"]:
                            item["pdfUrl"] = pdf_url
                        if "mediaUrl" not in item or not item["mediaUrl"]:
                            item["mediaUrl"] = pdf_url
                            item["mediaType"] = "pdf"
    except Exception:
        logger.warning(
            f"[course-job] trace_id={trace_id} post_process_warning",
            exc_info=True,
        )

    # Attach cohort_passwords
    try:
        if cohort_passwords and isinstance(course_obj, dict):
            course_obj["cohort_passwords"] = cohort_passwords
    except Exception:
        logger.warning(
            f"[course-job] trace_id={trace_id} failed attaching cohort_passwords"
        )

    # Log final JSON
    try:
        _serialized = _json.dumps(course_obj, ensure_ascii=False)
        _max_log = 20000
        if len(_serialized) > _max_log:
            logger.info(
                f"[course-job] trace_id={trace_id} final_course_json_len={len(_serialized)} "
                f"(truncated to {_max_log})"
            )
            logger.info(
                f"[course-job] trace_id={trace_id} final_course_json_truncated={_serialized[:_max_log]}"
            )
        else:
            logger.info(
                f"[course-job] trace_id={trace_id} final_course_json_len={len(_serialized)}"
            )
            logger.info(
                f"[course-job] trace_id={trace_id} final_course_json={_serialized}"
            )
    except Exception as _e:
        logger.warning(
            f"[course-job] trace_id={trace_id} final_json_log_error={str(_e)}"
        )

    logger.info(f"[course-job] trace_id={trace_id} success returning course from job")

    # IMPORTANT: return a dict with course + trace_id, not just the course
    return {"course": course_obj, "trace_id": trace_id}
