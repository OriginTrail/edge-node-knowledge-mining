from plugins.services.open_ai_client import call_openai_api
import logging
import json
from datetime import datetime


def split_abstract(abstract, max_length=800):
    logging.info(f"Length of abstract: {len(abstract)}")
    return [abstract[i : i + max_length] for i in range(0, len(abstract), max_length)]


def json_arr_to_ka(json_arr, file_name, llm_model):
    ka = {
        "@context": "http://schema.org",
        "@type": "DigitalDocument",
        "name": file_name,
        "fileFormat": "application/pdf",
        "headline": "",
        "abstract": "",
        "contentType": "",
        "hasPart": [],
    }

    # Concatenate all texts
    full_text = ""
    for chunk in json_arr:
        full_text += chunk.get("text", "") + " "

    # Chunk size 1000 with overlap 250
    chunk_size = 1000
    overlap = 250
    start = 0

    while start < len(full_text):
        end = start + chunk_size
        text_chunk = full_text[start:end]

        part = {
            "text": text_chunk,
        }

        ka["hasPart"].append(part)

        # Update the start position for the next chunk
        start += chunk_size - overlap

    prompt = f"""
    You are given the first 200 elements of a JSON array, where each element represents a portion of a parsed PDF. Each element contains parsed text of the PDF.

    Your task is to analyze these elements to generate a headline and an abstract for the PDF. The headline should be a concise, descriptive title that encapsulates the main topic of the document. The abstract should be a brief summary, typically 3-5 sentences, that highlights the key points, findings, or themes of the PDF.

    ** Instructions **:
    Headline: Analyze the text fields across the 200 elements and identify the most relevant title or topic that represents the entire PDF.
    Abstract: Based on the text content in the elements, construct a brief abstract that summarizes the main points of the PDF.
    ContentType: Based on the content of the JSON object, determine the most appropriate content type (e.g., Dataset, Report, Article) that belongs to schema.org ontology, and return it as the third row. The content type should reflect the nature and purpose of the JSON content.

    ** Actual input **
    Here is the JSON array for you to analyze:
    {[elem["text"] for elem in json_arr[0:200]]}

    Make sure to answer in three rows:
    1. First row should be the headline.
    2. Second row should be the abstract - a detailed narrative that summarizes the content of the JSON object.
    3. Third row should be the inferred content type from schema.org (e.g., Dataset, Report, Article, Book, Movie).

    ** Output format **
    Make sure to answer in two rows - first row should be the headline and the second row should be the abstract.

    ** Example outputs **
    Output 1:
    "Here you will put the headline
    Here you will put the abstract
    Dataset"

    Output 2:
    "Headline 1 example
    Abstract 1 example
    Report"
    """

    logging.info(f"Generating headline and abstract using {llm_model}")
    response_text = call_openai_api(llm_model, prompt)

    response_lines = response_text.split("\n")

    # Extract headline and abstract
    ka["headline"] = response_lines[0].strip()
    abstract = response_lines[1].strip()
    ka["abstract"] = split_abstract(abstract)
    ka["contentType"] = response_lines[2].strip().replace('"', "")

    return ka


def simple_json_to_ka(json_content, file_name, llm_model):
    ka = {
        "@context": "http://schema.org",
        "@type": "DigitalDocument",
        "name": file_name,
        "fileFormat": "application/json",
        "headline": "",
        "abstract": "",
        "contentType": "",
    }

    prompt = f"""
    You are provided with a JSON object that represents a highly structured dataset. This object is rich in information, capturing various aspects related to a particular topic. Your task is to carefully analyze the contents of this JSON object and generate a detailed summary that conveys the essence of what the data represents.

    ** Instructions **:
    1. Read through the text content in the JSON object, considering all elements that contribute to the overall understanding of the topic.
    2. Craft a comprehensive narrative that encapsulates the core ideas, themes, and insights present in the dataset.
    3. The summary should not merely describe the structure of the JSON object (e.g., "It has property X and Y"), but rather tell a coherent story that a human reader can easily understand. Focus on delivering a clear, meaningful, and engaging summary of the information, as if you were explaining the key points to someone unfamiliar with the content.
    4. Capture as much information as you can from the JSON object for the abstract - it is totally alright that the abstract is long.
    5. Based on the content of the JSON object, determine the most appropriate content type (e.g., Dataset, Report, Article) that belongs to schema.org ontology, and return it as the third row. The content type should reflect the nature and purpose of the JSON content.

    ** Actual input **:
    Here is the text content from the JSON object for your analysis:
    {json_content}

    ** Output format **:
    Make sure to answer in three rows:
    1. First row should be the headline.
    2. Second row should be the abstract - a detailed narrative that summarizes the content of the JSON object. Name/title of entity or any kind of quoted words should be quoted using single qoutes.
    3. Third row should be the inferred content type from schema.org (e.g., Dataset, Report, Article, Book, Movie).

    ** Example output **:
    Output 1:
    "Title of the Document (headline)
    This document provides a thorough exploration of... (abstract)
    Dataset"

    Output 2:
    "Key Findings on Topic X (headline)
    The dataset reveals critical insights into... (abstract)
    Report"
    """

    logging.info(f"Generating headline, abstract, and content type using {llm_model}")
    response_text = call_openai_api(llm_model, prompt)
    logging.info(f"Got response from {llm_model} - {response_text}")

    response_lines = response_text.split("\n")

    # Extract headline, abstract, and content type
    ka["headline"] = response_lines[0].strip()
    abstract = response_lines[1].strip()
    ka["abstract"] = split_abstract(abstract)
    ka["contentType"] = response_lines[2].strip().replace('"', "")

    return ka
