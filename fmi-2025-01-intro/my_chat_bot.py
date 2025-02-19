import ollama
import speech_recognition as sr
import sounddevice as sd


class ChatBot:
    def __init__(self, name, model):
        print(" --- Starting up ", name, " ---")
        self.name = name
        self.model = model
        self.recognizer = sr.Recognizer()

    def speech_to_text(self):
        print('Listening ...')
        with sr.Microphone() as mic:
            self.recognizer.adjust_for_ambient_noise(mic, duration=0.5)
            audio = self.recognizer.listen(mic)
        try:
            text = self.recognizer.recognize_google(audio, language='en')
            return text
        except Exception as ex:
            print("You -> Recognition Error", ex)


    def text_to_speech(self, message):
        pass

    def run(self):
        text = self.speech_to_text() # input('>')
        if text is not None:
            print(text)
            resp = ollama.chat(model=self.model, messages=[
                {
                'role': 'system',
                'content': 'You are an AI assistant'
                },
                {
                'role': 'user',
                'content': text
                }
            ])
        text = resp['message']['content']
        print(text)

if __name__ == "__main__":
    # ChatBot demo
    sophia = ChatBot('Sophia', 'llama3.2')
    sophia.run()
