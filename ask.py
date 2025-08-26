# ask.py
import sys
import requests

API_URL = "http://127.0.0.1:5001/ask"

def main():
    if len(sys.argv) < 2:
        print("❌ Bitte gib eine Frage an, z.B.:")
        print("   python ask.py \"Wer ist der aktivste User?\"")
        return

    frage = " ".join(sys.argv[1:])
    print(f"🔎 Frage: {frage}\n")

    response = requests.post(API_URL, json={"question": frage})

    if response.status_code != 200:
        print(f"❌ Fehler {response.status_code}: {response.text}")
        return

    data = response.json()
    print(f"💡 Antwort: {data['answer']}")

if __name__ == "__main__":
    main()
