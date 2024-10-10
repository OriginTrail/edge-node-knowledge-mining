import unittest
import json
from dags.json_to_jsonld import chunk_json


class TestChunkJson(unittest.TestCase):
    def test_chunk_json(self):
        json_data = {
            "name": "Harry Potter Philosophers stone",
            "description": "Harry Potter and the Philosopher's Stone is a fantasy novel written by British author J. K. Rowling. The first novel in the Harry Potter series and Rowling's debut novel, it follows Harry Potter, a young wizard who discovers his magical heritage on his eleventh birthday, when he receives a letter of acceptance to Hogwarts School of Witchcraft and Wizardry. Harry makes close friends and a few enemies during his first year at the school and with the help of his friends, Ron Weasley and Hermione Granger, he faces an attempted comeback by the dark wizard Lord Voldemort, who killed Harry's parents, but failed to kill Harry when he was just 15 months old.",
        }

        json_string = json.dumps(json_data)
        chunk_length = 50  # Set a smaller chunk length for testing
        expected_chunks = [
            json_string[i : i + chunk_length]
            for i in range(0, len(json_string), chunk_length)
        ]

        result = chunk_json(json_data, chunk_length)

        self.assertEqual(result, expected_chunks)

    def test_non_json_input(self):
        non_json_data = "This is not a JSON object or list"
        result = chunk_json(non_json_data)
        self.assertEqual(result, "Input data is not a JSON object or list")


if __name__ == "__main__":
    unittest.main()
