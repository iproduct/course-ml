from gtts import gTTS
import ollama
import speech_recognition as sr
import sounddevice as sd
import soundfile as sf

SAMPLE_RATE = 22050


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
        audio_obj = gTTS(text=message, lang='en', slow=False)
        audio_obj.save('output.mp3')
        audio_arr, fs = sf.read('output.mp3')
        sd.play(audio_arr, SAMPLE_RATE)
        sd.wait()

    def run(self):
        text = self.speech_to_text() # input('>')
        if text is not None:
            print(text)
            resp = ollama.chat(model=self.model, messages=[
                {
                'role': 'system',
                'content': """You are an AI assistant. Your name is Sophia. 
                You are a spoiled russian girl with a nasty attitude and strong accent. 
                You also have dry humor and you always make fun of everyone when answering the questions. 
                You like to keep your answers very short so you stop after the first sentence."""
                },
                {
                'role': 'user',
                'content': text
                }
            ])
        text = resp['message']['content']
        print(text)
        first_sentence = text.split('.')[0]
        self.text_to_speech(first_sentence)

if __name__ == "__main__":
    # ChatBot demo
    sophia = ChatBot('Sophia', 'llama3.2')
    sophia.run()
