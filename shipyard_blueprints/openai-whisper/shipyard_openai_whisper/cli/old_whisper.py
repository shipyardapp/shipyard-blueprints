import os
import whisper


def main():
    audio_file = os.environ.get('WHISPER_FILE')
    exported_file = os.environ.get('WHISPER_DESTINATION_FILE_NAME')

    model = whisper.load_model('base')
    result = model.transcribe(audio_file)

    with open(exported_file, 'w') as f:
        f.write(result['text'])


if __name__ == '__main__':
    main()
