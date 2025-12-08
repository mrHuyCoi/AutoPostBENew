import json
from typing import Optional, Dict, Union, List

from openai import AsyncOpenAI
from app.configs.settings import settings
import logging
import re
from google.generativeai.types import HarmCategory, HarmBlockThreshold

try:
    import google.generativeai as genai
    from google.generativeai.types import GenerationConfig
except ImportError:
    genai = None
    GenerationConfig = None

model_openai = "gpt-4o"
model_gemini = "gemini-2.5-flash"

logger = logging.getLogger(__name__)

class AIService:
    @staticmethod
    async def generate_content(
        prompt: str,
        ai_platform: str,
        temperature: float = 0.8,
        max_tokens: Optional[int] = 1024,
        platform: Optional[str] = None,
        brand_name: Optional[str] = None,
        posting_purpose: Optional[str] = None,
        call_to_action: Optional[str] = None,
        hashtags: Optional[List[str]] = None,
        custom_system_prompt: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Union[str, Dict]:
        try:
            if not api_key:
                raise ValueError("Thiếu API key.")

            # ==== System Prompt Setup ====
            default_prompt = (
                "Bạn là chuyên gia viết nội dung cho video và mạng xã hội. "
                "Hãy viết bài đăng ngắn gọn, hấp dẫn, dễ lan truyền và phù hợp với từng nền tảng. "
            )
            
            system_prompt = (
                custom_system_prompt.strip() if custom_system_prompt else default_prompt
            )
            # ==== Platform-specific Prompt Customization ====
            platform = platform or ""
            platform = platform.lower()

            if platform in ["facebook-reels", "instagram-reels"]:
                system_prompt += (
                    "\nWrite a neutral caption for a short-form Reels video on social media. The content should focus on sharing useful information that is relevant to the community. "
                )
                if brand_name:
                    system_prompt += f"\nBrand should have: {brand_name.strip()}"
                if posting_purpose:
                    system_prompt += f"\nPosting purpose: {posting_purpose.strip()}"
                if call_to_action:
                    system_prompt += f"\nA call-to-action can be included: {call_to_action.strip()}"
                if hashtags:
                    system_prompt += f"\nHashtags should be included: {' '.join(hashtags)}"

            elif platform in ["youtube"]:
                system_prompt += (
                    "Return the result in JSON format as follows (DO NOT include any other keys):\n"
                    '{\n  "title": "",\n  "description": "",\n  "tags": ["", ""]\n}'
                    "\nEach field must have a valid value, do not leave any field empty. Only return JSON, do not add any other explanations."
                )

            elif platform in ["facebook-page", "instagram-feed"]:
                system_prompt += (
                    "\nWrite a concise and engaging post for a Facebook Page or Instagram Feed. "
                )
                if brand_name:
                    system_prompt += f"\nBrand should have: {brand_name.strip()}"
                if posting_purpose:
                    system_prompt += f"\nPosting purpose: {posting_purpose.strip()}"
                if call_to_action:
                    system_prompt += f"\nA call-to-action can be included: {call_to_action.strip()}"
                if hashtags:
                    system_prompt += f"\nHashtags should be included: {' '.join(hashtags)}"
            full_prompt = ""
            full_prompt = f"{system_prompt}\n\nRespond in Vietnamese. Here is the prompt you need to answer: {prompt}"

            # ==== AI Platform Handling ====
            if ai_platform == "openai":
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=api_key)
                # Function schema cho YouTube
                youtube_function = {
                    "name": "create_youtube_post",
                    "description": "Sinh nội dung YouTube với title, description, tags",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string",
                                "description": "Tiêu đề video YouTube (≤100 ký tự)"
                            },
                            "description": {
                                "type": "string",
                                "description": "Mô tả chi tiết cho video YouTube"
                            },
                            "tags": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Danh sách thẻ tags cho video YouTube"
                            }
                        },
                        "required": ["title", "description", "tags"]
                    }
                }
                system_prompt += f"{system_prompt.strip()}. Use text formatting suitable for post in social platforms like Facebook, Instagram,..."
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ]
                if platform == "youtube":
                    response = await client.chat.completions.create(
                        model=model_openai,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        functions=[youtube_function],
                        function_call={"name": "create_youtube_post"}
                    )
                    # Lấy kết quả từ function_call
                    func_args = response.choices[0].message.function_call.arguments
                    try:
                        return json.loads(func_args)
                    except Exception:
                        return {
                            "title": "",
                            "description": func_args,
                            "tags": []
                        }
                else:   
                    response = await client.chat.completions.create(
                        model=model_openai,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens
                    )
                    content = response.choices[0].message.content.strip()
                    return content

            elif ai_platform == "gemini":
                import google.generativeai as genai
                from google.generativeai.types import GenerationConfig

                genai.configure(api_key=api_key)
                config = {
                    "temperature": temperature,
                    "max_output_tokens": max_tokens or 2048,
                }

                if platform == "youtube":
                    full_prompt += (
                        "Return the result in JSON format as follows (DO NOT include any other keys):\n"
                        '{\n  "title": "",\n  "description": "",\n  "tags": ["", ""]\n}'
                    )
                    config["response_mime_type"] = "application/json"
                
                my_safety_settings = [
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

                
                gemini_config = GenerationConfig(**config)
                model_instance = genai.GenerativeModel(
                    model_name=model_gemini,
                    generation_config=gemini_config,
                    safety_settings=my_safety_settings
                )

                chat = model_instance.start_chat(history=[])
                response = chat.send_message(full_prompt)

                content = response.text.strip()

                if platform == "youtube":
                    try:
                        data = json.loads(content)
                        result = AIService.extract_youtube_json(data)
                        if result:
                            return result
                    except json.JSONDecodeError:
                        match = re.search(r'\{[\s\S]*\}', content)
                        if match:
                            try:
                                data = json.loads(match.group(0))
                                result = AIService.extract_youtube_json(data)
                                if result:
                                    return result
                            except Exception:
                                pass
                        return {
                            "title": "Untitled",
                            "description": content,
                            "tags": []
                        }
                return content

            else:
                raise ValueError(f"`ai_platform` không hợp lệ: {ai_platform}")

        except Exception as e:
            logger.exception("Lỗi khi sinh nội dung AI")
            raise RuntimeError(f"Lỗi sinh nội dung AI: {str(e)}") from e

    @staticmethod
    def extract_youtube_json(obj):
        if isinstance(obj, dict) and set(obj.keys()) >= {"title", "description", "tags"}:
            return {
                "title": obj["title"],
                "description": obj["description"],
                "tags": obj["tags"]
            }
        if isinstance(obj, dict):
            for v in obj.values():
                res = AIService.extract_youtube_json(v)
                if res:
                    return res
        return None
