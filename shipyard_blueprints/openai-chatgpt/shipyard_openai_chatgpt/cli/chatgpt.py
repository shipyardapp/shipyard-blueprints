import os
import openai


def main():
    key = os.environ.get('CHATGPT_API_KEY')
    number_of_responses = int(os.environ.get('CHATGPT_RESPONSES'))
    randomness = float(os.environ.get('CHATGPT_RANDOMNESS'))
    prompt = os.environ.get('CHATGPT_PROMPT')
    file_name = os.environ.get('CHATGPT_DESTINATION_FILE_NAME')

    openai.api_key = key

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        n=number_of_responses,
        temperature=randomness,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    insert_location = file_name.find('.txt')

    for i in range(0, len(completion.choices)):
        if i == 0:
            print(f"This prompt will be saved in file: {file_name}")
            with open(f'{file_name}', 'w') as f:
                f.write(completion.choices[i].message.content)
        else:
            new_file_name = file_name[:insert_location] + f'_{i}' + file_name[insert_location:]
            print(f"This prompt will be saved in file: {new_file_name}")
            with open(f'{new_file_name}', 'w') as f:
                f.write(completion.choices[i].message.content)
        print(completion.choices[i].message.content)
