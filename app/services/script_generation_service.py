import json
import logging
from typing import List, Optional

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

try:
    from openai import AsyncOpenAI
    from openai import RateLimitError, APIError
except ImportError:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore
    RateLimitError = None  # type: ignore
    APIError = None  # type: ignore

try:
    import google.generativeai as genai
    from google.generativeai.types import GenerationConfig
    from google.generativeai.types import HarmBlockThreshold, HarmCategory
except ImportError:  # pragma: no cover
    genai = None  # type: ignore
    GenerationConfig = None  # type: ignore
    HarmCategory = None  # type: ignore
    HarmBlockThreshold = None  # type: ignore

MODEL_OPENAI = "gpt-4o"
MODEL_GEMINI = "gemini-2.5-flash"


class ScriptGenerationService:
    @staticmethod
    async def generate_script(
        video_subject: str,
        video_language: str,
        video_keywords: Optional[List[str]],
        script_style: Optional[str],
        paragraph_number: int,
        llm_provider: str,
        openai_key: Optional[str],
        gemini_key: Optional[str],
    ) -> str:
        provider = llm_provider.lower()
        api_key = ScriptGenerationService._get_api_key(provider, openai_key, gemini_key)

        prompt = ScriptGenerationService._build_prompt(
            subject=video_subject,
            language=video_language,
            keywords=video_keywords,
            style=script_style,
            paragraph_number=paragraph_number,
        )

        if provider == "openai":
            try:
                return await ScriptGenerationService._generate_with_openai(prompt, api_key, video_language)
            except HTTPException as e:
                # Nếu OpenAI hết quota (429) và có Gemini key, tự động fallback
                if e.status_code == status.HTTP_429_TOO_MANY_REQUESTS or "quota" in str(e.detail).lower() or "insufficient_quota" in str(e.detail).lower():
                    if gemini_key:
                        logger.warning(f"OpenAI hết quota, tự động chuyển sang Gemini. Lỗi: {e.detail}")
                        return await ScriptGenerationService._generate_with_gemini(prompt, gemini_key, video_language)
                raise
        if provider == "gemini":
            return await ScriptGenerationService._generate_with_gemini(prompt, api_key, video_language)

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="llm_provider không hợp lệ. Chỉ hỗ trợ: openai, gemini",
        )

    @staticmethod
    def _get_api_key(provider: str, openai_key: Optional[str], gemini_key: Optional[str]) -> str:
        if provider == "openai":
            if not openai_key:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Thiếu OpenAI API key.",
                )
            return openai_key
        if provider == "gemini":
            if not gemini_key:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Thiếu Gemini API key.",
                )
            return gemini_key
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="llm_provider không hợp lệ.",
        )

    @staticmethod
    def _build_prompt(
        subject: str,
        language: str,
        keywords: Optional[List[str]],
        style: Optional[str],
        paragraph_number: int,
    ) -> str:
        keywords_text = ", ".join(keywords) if keywords else ""
        style_text = style or "tự nhiên, súc tích"

        prompt_lines = [
            "Bạn là chuyên gia viết kịch bản video chuyên nghiệp. Viết kịch bản NGẮN GỌN, SÚC TÍCH, đi thẳng vào nội dung chính.",
            "",
            "QUAN TRỌNG - YÊU CẦU FORMAT:",
            "1. KHÔNG viết phần giới thiệu dài dòng như 'Tuyệt vời! Với vai trò là...' hoặc 'Tôi sẽ giúp bạn...'",
            "2. BẮT ĐẦU trực tiếp với tiêu đề video hoặc phần mở đầu ngắn gọn (1-2 câu)",
            "3. Sử dụng format markdown đơn giản: **Tiêu đề**, - Danh sách, [Thời gian] cho phân đoạn",
            "4. Mỗi đoạn chỉ 2-4 câu, tập trung vào thông tin chính, bỏ chi tiết dư thừa",
            "5. Sử dụng bullet points (-) thay vì đoạn văn dài",
            "",
            "THÔNG TIN VIDEO:",
            f"- Chủ đề: {subject}",
            f"- Ngôn ngữ: {language} (viết hoàn toàn bằng ngôn ngữ này)",
            f"- Số đoạn: {paragraph_number}",
        ]
        
        if keywords_text:
            prompt_lines.append(f"- Từ khóa: {keywords_text}")
        
        prompt_lines.extend([
            f"- Phong cách: {style_text}",
            "",
            "CẤU TRÚC KỊCH BẢN:",
            "1. [MỞ ĐẦU] - 1-2 câu giới thiệu ngắn gọn",
            f"2. [NỘI DUNG] - Chia thành {paragraph_number} đoạn chính, mỗi đoạn 2-4 câu",
            "3. [KẾT THÚC] - 1-2 câu kêu gọi hành động (like, subscribe, comment)",
            "",
            "LƯU Ý:",
            "- Viết súc tích, không lan man",
            "- Tập trung vào thông tin giá trị, bỏ phần giải thích dư thừa",
            "- Format rõ ràng, dễ đọc, dễ quay video",
        ])

        return "\n".join(prompt_lines)

    @staticmethod
    async def _generate_with_openai(prompt: str, api_key: str, language: str) -> str:
        if AsyncOpenAI is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Thư viện OpenAI chưa được cài đặt.",
            )

        try:
            client = AsyncOpenAI(api_key=api_key)
            system_prompt = (
                "Bạn là chuyên gia viết kịch bản video chuyên nghiệp. "
                "QUAN TRỌNG: Viết NGẮN GỌN, SÚC TÍCH, đi thẳng vào nội dung chính. "
                "KHÔNG viết phần giới thiệu dài dòng. Tuân thủ chính xác format và yêu cầu được cung cấp."
            )
            language_enforcement = (
                f"Luôn trả lời hoàn toàn bằng ngôn ngữ: {language}. "
                "Nếu có hướng dẫn ngôn ngữ mâu thuẫn, hãy ưu tiên yêu cầu này."
            )

            messages = [
                {"role": "system", "content": f"{system_prompt}\n{language_enforcement}"},
                {"role": "user", "content": prompt},
            ]
            response = await client.chat.completions.create(
                model=MODEL_OPENAI,
                messages=messages,
                temperature=0.7,
                max_tokens=4000,  # Tăng để đảm bảo đủ cho kịch bản chi tiết
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:  # pragma: no cover - runtime errors
            logger.exception("Lỗi sinh kịch bản với OpenAI")
            
            # Kiểm tra xem có phải là RateLimitError hoặc APIError không
            is_rate_limit_error = (
                RateLimitError is not None and isinstance(exc, RateLimitError)
            ) or (
                APIError is not None and isinstance(exc, APIError)
            )
            
            # Phát hiện lỗi quota trong message hoặc error code
            error_code = getattr(exc, 'status_code', None) or getattr(exc, 'code', None)
            error_str = str(exc).lower()
            
            if is_rate_limit_error or error_code == 429 or "quota" in error_str or "insufficient_quota" in error_str or "rate_limit" in error_str:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="OpenAI API key đã hết quota/hạn mức. Vui lòng kiểm tra billing hoặc sử dụng Gemini API key thay thế.",
                ) from exc
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi sinh kịch bản với OpenAI: {exc}",
            ) from exc

    @staticmethod
    async def _generate_with_gemini(prompt: str, api_key: str, language: str) -> str:
        if genai is None or GenerationConfig is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Thư viện Google Generative AI chưa được cài đặt.",
            )

        try:
            genai.configure(api_key=api_key)
            config = GenerationConfig(
                temperature=0.7,
                max_output_tokens=8192,  # Tăng để tránh bị cắt ở MAX_TOKENS
            )

            safety_settings = [
                {
                    "category": HarmCategory.HARM_CATEGORY_HARASSMENT,
                    "threshold": HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    "category": HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    "threshold": HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    "category": HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    "threshold": HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    "category": HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    "threshold": HarmBlockThreshold.BLOCK_NONE,
                },
            ]

            language_instruction = (
                f"Hãy trả lời hoàn toàn bằng ngôn ngữ: {language}. "
                "Nếu có hướng dẫn ngôn ngữ khác, hãy bỏ qua và ưu tiên yêu cầu này."
            )

            model = genai.GenerativeModel(
                model_name=MODEL_GEMINI,
                generation_config=config,
                safety_settings=safety_settings,
            )
            chat_session = model.start_chat(history=[])
            response = chat_session.send_message(f"{language_instruction}\n{prompt}")
            text = ScriptGenerationService._extract_text_from_gemini(response)

            if not text:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Gemini trả về nội dung trống.",
                )

            return text
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover - runtime errors
            logger.exception("Lỗi sinh kịch bản với Gemini")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi sinh kịch bản với Gemini: {exc}",
            ) from exc

    @staticmethod
    def _extract_text_from_gemini(response) -> str:
        """Chuyển đổi phản hồi Gemini thành văn bản, xử lý các trường hợp bị chặn."""
        if not response:
            return ""

        prompt_feedback = getattr(response, "prompt_feedback", None)
        if prompt_feedback and getattr(prompt_feedback, "block_reason", None):
            reason = getattr(prompt_feedback.block_reason, "name", str(prompt_feedback.block_reason))
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Prompt bị chặn bởi Gemini (prompt block reason: {reason}).",
            )

        candidates = getattr(response, "candidates", None)
        if not candidates:
            return getattr(response, "text", "") or ""

        safety_errors: list[str] = []
        text_parts: list[str] = []

        for candidate in candidates:
            finish_reason = getattr(candidate, "finish_reason", None)
            
            # Kiểm tra finish_reason là SAFETY (có thể là số 2, string "SAFETY", hoặc enum)
            is_safety = False
            if finish_reason == 2:  # Số 2
                is_safety = True
            elif hasattr(finish_reason, 'name') and finish_reason.name == 'SAFETY':  # Enum
                is_safety = True
            elif str(finish_reason) == 'SAFETY' or str(finish_reason) == '2':  # String
                is_safety = True
            
            if is_safety:
                safety_ratings = getattr(candidate, "safety_ratings", []) or []
                blocked_categories = [
                    f"{rating.category} (prob: {rating.probability.name})"
                    for rating in safety_ratings
                    if getattr(rating, "blocked", False)
                ]
                if blocked_categories:
                    safety_errors.append(", ".join(blocked_categories))
                continue

            # Với các finish_reason khác (MAX_TOKENS, STOP, etc.), vẫn trích xuất text
            content = getattr(candidate, "content", None)
            parts = getattr(content, "parts", []) if content else []
            for part in parts:
                text = getattr(part, "text", None)
                if text:
                    text_parts.append(text)

        if safety_errors and not text_parts:
            detail = "Nội dung bị chặn bởi bộ lọc an toàn của Gemini."
            if safety_errors:
                detail += f" Danh mục bị chặn: {', '.join(safety_errors)}."
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=detail,
            )

        return "\n".join(text_parts).strip()

