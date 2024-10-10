from pyld import jsonld
from json.decoder import JSONDecodeError
import json
import logging


def is_valid_jsonld(content):
    try:
        # Load the content
        data = json.loads(content)

        normalization_options = {
            "algorithm": "URDNA2015",
            "format": "application/n-quads",
        }

        try:
            # Try to normalize the JSON-LD data
            # TODO: fix cors error
            n_quads = jsonld.normalize(data, normalization_options)
            assertion = [quad for quad in n_quads.split("\n") if quad]

            if not assertion:
                raise Exception("Invalid dataset, no quads were extracted.")

        except jsonld.JsonLdError as e:
            logging.warning(f"Remote context issue: {str(e)}")

            # Fallback to basic JSON-LD validation without normalization
            if "@context" in data and "@type" in data:
                logging.info("Fallback validation succeeded without normalization.")
                return True
            else:
                logging.error("Fallback validation failed: missing @context or @type.")
                return False

        return True

    except Exception as e:
        logging.error(f"Invalid JSON-LD content error: {str(e)}")
        return False
