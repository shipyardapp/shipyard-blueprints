import os
import openai

key = os.environ.get('CHATGPT_API_KEY')
original_text = os.environ.get('CHATGPT_TEXT_FILE')
lang = os.environ.get('CHATGPT_LANGUAGE')
export_file = os.environ.get('CHATGPT_DESTINATION_FILE_NAME')


openai.api_key = key
language = lang

with open(original_text) as f:
    lines = f.readlines()
text = lines[0]

completion = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  n = 1,
  temperature = 1,
  messages=[
    {"role": "system", "content": f"You are an assistant that translates the given text to {language}."},
    {"role": "user", "content": f"{text}"}
  ]
)

with open(export_file,'w') as f:
    f.write(completion.choices[0].message.content)