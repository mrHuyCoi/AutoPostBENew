import os 
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.generate.llm.gpt import generate_metadata
import json 
from dotenv import load_dotenv
load_dotenv()

def test_openai():
  # Parse kết quả
  completion = generate_metadata(content="Model tạo video mới của google Veo3")
  response_arguments = completion.choices[0].message.function_call.arguments
  metadata = json.loads(response_arguments)

  # In ra kết quả
  print("🎯 Title:", metadata["title"])
  print("📝 Description:", metadata["description"])
  print("🏷️ Tags:", ", ".join(metadata["tags"]))