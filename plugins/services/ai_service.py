import json
import openai
from plugins.utils.ontology_utils import get_ontology_by_name
from plugins.services.open_ai_client import call_openai_api
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from plugins.utils.prompt_utils import ontology_examples, vectorize_jsonld_examples
from plugins.utils.ontology_utils import category_ontology_map


# ~17K characters is about 4096 input tokens, which is the context window of gpt3.5turbo. Limitting to around 10k to allow for ontologies to be passed in the context too
def chunk_json(json_data, chunk_length=10000):
    if not isinstance(json_data, (dict, list)):
        return "Input data is not a JSON object or list"

    json_string = json.dumps(json_data)

    chunked_strings = [
        json_string[i : i + chunk_length]
        for i in range(0, len(json_string), chunk_length)
    ]

    logging.info(f"Chunked JSON into {len(chunked_strings)} chunks")

    return chunked_strings


def transform_chunk_using_ontology(model, chunk, ontology, llm_api_key=None):
    prompt = f"""
    {ontology}

    Given the provided ontology, generate a JSON representation of the provided content: {chunk}. Ensure that the JSON representation only includes information that is specified in the ontology and nothing else. Capture as much relevant information as possible based on the ontology's classes and properties.
    
    ### Instructions

    1. **Understand the Ontology**: Recognize the key classes and properties defined in the ontology.
    2. **Extract Relevant Information**: Identify and extract information from the provided content that corresponds to the ontology's classes and properties.
    3. **Generate JSON**: Create a JSON representation that includes only the information specified in the ontology.

    Make sure to ONLY OUTPUT THE JSON, DONT output any other remarks or comments.
    """

    logging.info(f"Transforming chunk to ontology using {model}")
    response_text = call_openai_api(model, prompt, llm_api_key)

    # Ensure that JSON can be parsed later
    if response_text.startswith("```json") and response_text.endswith("```"):
        response_text = response_text[7:-3].strip()

    return response_text


def transform_chunks_into_rdf(model, chunks, ontology, ontology_name, llm_api_key=None):
    prompt = f"""
    {ontology}

    Given the provided ontology, generate an array of JSON-LD RDF representations for the entities extracted from the provided JSON content array: {str(chunks)}. Ensure that each JSON-LD object only includes information that is specified in the ontology and nothing else. Capture as much relevant information as possible based on the ontology's classes and properties.
    
    ### Instructions

     1. **Understand the Ontology**: Recognize the key classes and properties defined in the ontology.
    2. **Extract Entities**: Identify and extract distinct entities from the provided content.
    3. **Transform Relevant Information**: Identify and transform information from the extracted entities that corresponds to the ontology's classes and properties from regular JSON to RDF JSON-LD format.
    4. **Generate JSON-LD Objects**: Create separate JSON-LD RDF representations for each extracted entity, ensuring each object includes only the information specified in the ontology, based on the transformed relevant information.
    5. **Include Mandatory Fields**: Make sure to include the '@context', '@type' and '@id' fields in each JSON-LD object - use valid @id's, do not use blank node ids and make sure to connect them properly using the @ids. Make sure to use links from the input context as @id's if they are relevant.
    6. **Avoid Repetition**: Do not repeat content across JSON-LD objects; ensure each object is unique and captures distinct information. E.g. to represent a single book, use a single object, do not represent the same book using different objects. However, you can use another object to represent the author of the book.
    7. **Return JSON-LD Array**: Output an array of unique JSON-LD objects representing all extracted entities from the provided JSON content.

    ### Output Format
    Output the array of JSON-LD objects directly, without any additional formatting such as wrapping in ```json ``` or any extra comments.

    ### Example output for {ontology_name} ontology:
    {str(ontology_examples[ontology_name])}
    """

    logging.info(f"Transforming chunk to JSON-LD RDF format with using {model}")
    response_text = call_openai_api(model, prompt, llm_api_key)

    # Ensure that JSON can be parsed later
    if response_text.startswith("```json") and response_text.endswith("```"):
        response_text = response_text[7:-3].strip()

    return response_text


def choose_ontology_from_category(model, chunk, category, llm_api_key=None):
    ontologies_for_category = category_ontology_map.get(category, [])

    if len(ontologies_for_category) == 0:
        logging.error("That category is not supported.")
    elif len(ontologies_for_category) == 1:
        logging.info(f"Chosen ontology {ontologies_for_category[0]}")
        return ontologies_for_category[0]
    else:
        prompt = f"""
        You are provided with a list of ontologies and a JSON chunk. Your task is to determine which ontology is most suitable for modeling the given chunk into an RDF graph.

        ### Ontologies Available:
        {str(ontologies_for_category)}

        ### JSON Chunk:
        {str(chunk)}

        **Instructions:**
        1. **Understand Ontologies:** Review the list of available ontologies provided above.
        2. **Analyze the JSON Chunk:** Examine the provided JSON chunk to understand its structure and content.
        3. **Match Content to Ontology:** Identify which ontology best fits the JSON chunk based on the classes and properties it describes.
        
        **Output Requirement:**
        - Output only the name of the chosen ontology.
        - DO NOT include any additional remarks or comments in your response.
        
        Ensure your output is precise and only includes the name of the ontology.
        """

        response_text = call_openai_api(model, prompt, llm_api_key)
        logging.info(f"Chosen ontology {response_text}")
        return response_text


def transform_chunk_to_json_ld(model, chunk, category, llm_api_key=None):
    selected_ontology = choose_ontology_from_category(
        model, chunk, category, llm_api_key
    )

    if not selected_ontology:
        logging.error("No suitable ontology found for the chunk.")
        return None

    ontology = get_ontology_by_name(selected_ontology)

    transformed_chunk = transform_chunk_using_ontology(
        model, chunk, ontology, llm_api_key
    )

    if transformed_chunk:
        final_json_ld = transform_chunks_into_rdf(
            model, [transformed_chunk], ontology, selected_ontology, llm_api_key
        )
        return final_json_ld

    return None


def transform_chunks_to_ontology(model, chunked_content, category, llm_api_key=None):
    responses = []

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                transform_chunk_to_json_ld, model, chunk, category, llm_api_key
            )
            for chunk in chunked_content
        ]

        for future in as_completed(futures):
            try:
                response = future.result()
                responses.append(response)
            except Exception as e:
                logging.error(f"Error processing chunk: {e}")

    parsed_responses = []
    for response in responses:
        try:
            parsed_response = json.loads(response)

            parsed_responses.append(parsed_response)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e}")
            continue

    # Flatten array
    parsed_responses = [item for sublist in parsed_responses for item in sublist]

    # Convert the list of parsed JSON objects to a JSON string
    final_json_ld = json.dumps(parsed_responses, indent=2)

    # Log the final JSON-LD object
    logging.info(f"Got final JSON-LD object: {final_json_ld}")


def get_fields_to_vectorize(model, knowledge_asset, llm_api_key=None):
    prompt = f"""
        You are a part of an AI application which handles vectorization of different JSON LD objects. You are provided with a JSON-LD object.
        Your task is to determine which fields of the JSON-LD objects should be vectorized. Fields that contain semantic meaning and longer text are the ones you should choose to vectorize.

        ### JSON-LD object:
        {str(knowledge_asset)}

        ### Instructions:
        1. **Identify Relevant Fields**: Choose fields that contain semantic meaning and longer text. Typically, these fields are descriptions and longer bodies of text, or any other fields containing significant contextual information.
        2. **Exclude Non-Descriptive Fields**: Avoid selecting fields that are identifiers, short labels, dates, numerical values, or other non-descriptive data.
        3. **Multiple Fields**: If there are multiple fields that meet the criteria, include all of them for vectorization in the provided output format.

        ### Output Requirement:**
        - Return array of texts to vectorize e.g. ["text1", "text2", "text3"]
        - Return ONLY that array, no other remarks or comments.

        ** Examples of correct output **

        1) ["text1", "text2", "text3"]
        2) []
        3) ["texttexttexttext"]

        ** Examples of INCORRECT output **

        1) 'Here is your output: ["text1", "text2", "text3"]'
        2) ```json []```
        3) Here is your JSON output: "texttexttexttext"


        ### Examples:
        
        ** Example 1 **
        Input JSON-LD object:
        {str(vectorize_jsonld_examples[0])}

        Output:
        ["Building Name: Eiffel Tower, Address: Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France, Owner: City of Paris", "Construction Costs: [1887: 1000000 FRF, \"Initial construction costs for the foundation\"], [1888: 2000000 FRF, \"Construction costs for the first and second levels\"], [1889: 1500000 FRF, \"Final construction costs for the completion of the tower\"]. Maintenance Costs: [2022: 500000 EUR, \"Annual maintenance and painting\"], [2023: 600000 EUR, \"Structural inspections and minor repairs\"]. Funding Sources: [Government Grant: 2000000 EUR, \"Annual grant for maintenance and operations\"], [Tourism Revenue: 1000000 EUR, \"Revenue from ticket sales and concessions\"]"]

        Explanation:
        These two fields capture the most semantic meaning among the fields and contain a decent amount of text.

        ** Example 2 **

        Input JSON-LD object:
        {str(vectorize_jsonld_examples[1])}

        Output:
        []

        Explanation:
        None of these fields capture any semantic meaning by themselves so there is no need to include any of them.

        ** Example 3 **

        Input JSON-LD object:
        {str(vectorize_jsonld_examples[2])}

        Output:
        ["This is a building diagnosis and material study for Hospital Real, an ancient hospital in Granada built in 1504 following a design by Enrique Egas (late Gothic), although Charles V finished the project in Renaissance style. It is currently the main headquarters of the University of Granada, home to the Rectorate and other central university services. A more detailed description of the building is here: https://patrimonio.ugr.es/bien-inmueble/610/ To my best knowledge, the building has a permanent structural health monitoring system installed. Also, in November 2023, it was fully surveyed by researchers from University of Granada, and the natural vibration modes of the building were identified.", "Hospital Real Facade - building diagnosis and material study"]

        Explanation:
        The description field captures semantic meaning by itself. Name field is also included as it can be very useful to provide context, even though the name itself might not provide full semantic meaning.

        Ensure your output is precise and only includes the name of the ontology.
        """

    response_text = call_openai_api(model, prompt, llm_api_key)
    print(f"Got texts to vectorize: {response_text}")
    logging.info(f"Got texts to vectorize: {response_text}")
    return response_text
