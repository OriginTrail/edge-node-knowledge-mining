import unittest
from unittest.mock import patch, MagicMock
from plugins.services.ai_service import (
    transform_chunks_to_ontology,
    chunk_json,
)


class TestTransformChunksToOntology(unittest.TestCase):
    def test_transform_chunks_to_ontology(self):
        # Example input
        json_data = {
            "name": "Harry Potter Philosophers stone",
            "description": "Harry Potter and the Philosopher's Stone is a fantasy novel written by British author J. K. Rowling. The first novel in the Harry Potter series and Rowling's debut novel, it follows Harry Potter, a young wizard who discovers his magical heritage on his eleventh birthday, when he receives a letter of acceptance to Hogwarts School of Witchcraft and Wizardry. Harry makes close friends and a few enemies during his first year at the school and with the help of his friends, Ron Weasley and Hermione Granger, he faces an attempted comeback by the dark wizard Lord Voldemort, who killed Harry's parents, but failed to kill Harry when he was just 15 months old.",
        }
        chunked_content = chunk_json(json_data, 350)
        selected_ontology = "BIBO"
        model = "gpt-3.5-turbo"

        # Call the transform_chunks_to_ontology function
        responses = transform_chunks_to_ontology(
            model, chunked_content, selected_ontology
        )

        self.assertEqual(len(responses), len(chunked_content))
        for response in responses:
            self.assertIsInstance(response, str)


if __name__ == "__main__":
    unittest.main()
