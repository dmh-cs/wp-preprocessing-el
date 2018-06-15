import data_cleaners as dc
import pydash as _

def test__clean_sentence():
  sentence = {"text": "Die Parade tussen Plein-, Buitenkant-, Darling- en Kasteelstraat in ( ; Kaapstad) word as markplein, parkeerterrein en vir massabyeenkomste gebruik.",
              "links": [{"page": "Kaapstad", "text": "Kaapstad"}]}
  result = dc._clean_sentence(sentence)
  clean_sentence = {"text": "Die Parade tussen Plein-, Buitenkant-, Darling- en Kasteelstraat in  word as markplein, parkeerterrein en vir massabyeenkomste gebruik.",
                    "links": []}
  assert result == clean_sentence
