import os
import json
from openai import OpenAI
from app.generate.prompt.youtube import functions
from dotenv import load_dotenv
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_metadata(content):
    # Gửi request đến OpenAI API
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Bạn là một trợ lý sáng tạo nội dung cho các nội dung trên youtube"},
            {
                "role": "user",
                "content": f"Hãy tạo title, description và tags cho nội dung sau: {content}"
            }
        ],
        functions=functions,
        function_call={"name": "generate_metadata"}
    )
    return completion
