import os
import openai
openai.api_key = os.environ.get('CHATGPT_API_KEY')

message_type = os.environ.get('CHATGPT_MESSAGE_TYPE')
impersonation = os.environ.get('CHATGPT_IMPERSONATION')
speaking_tone = os.environ.get('CHATGPT_TONE')
recipient = os.environ.get('CHATGPT_RECIPIENT')
sender = os.environ.get('CHATGPT_SENDER')
prompt = os.environ.get('CHATGPT_PROMPT')
export_file = os.environ.get('CHATGPT_DESTINATION_FILE_NAME')

if recipient != '':
    gpt_to = f'to {recipient}'
else:
    gpt_to = ''

if sender == '':
    gpt_from = ''
else:
    gpt_from = f"from {sender}"

if impersonation == '':
    speaking_type = ''
else:
    speaking_type = f'in the same speaking style as {impersonation}'

if speaking_tone == '':
    tone = ''
else:
    tone = f'with a {speaking_tone} tone'

if message_type == 'email':
    message = f"Write an email {gpt_to} saying {prompt} {gpt_from} without a subject line {speaking_type} {tone}. The email should not have a closing."
else:
    message = f"Write a direct message {gpt_to} saying {prompt} {gpt_from} {speaking_type} {tone}"


completion = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=[
    {"role": "user", "content": message}
  ]
)


print(completion.choices[0].message.content)

with open(export_file,'w') as f:
    f.write(completion.choices[0].message.content)