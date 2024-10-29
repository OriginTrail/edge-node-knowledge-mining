import json
from unstructured.partition.api import partition_via_api
from unstructured.staging.base import elements_to_json
from unstructured.cleaners.core import clean_bullets, clean_extra_whitespace
from pypdf import PdfReader, PdfWriter
import io
import logging
import os
from dotenv import load_dotenv

load_dotenv()
api_url = os.getenv("UNSTRUCTURED_API_URL")
api_key = os.getenv("UNSTRUCTURED_API_KEY")


def split_pdf(file, max_pages_per_part=25):
    logging.info("Splitting pdf...")
    pdf_reader = PdfReader(file)
    parts = []
    total_pages = len(pdf_reader.pages)

    for start_page in range(0, total_pages, max_pages_per_part):
        pdf_writer = PdfWriter()
        end_page = min(start_page + max_pages_per_part, total_pages)

        for page in range(start_page, end_page):
            pdf_writer.add_page(pdf_reader.pages[page])

        part_pdf = io.BytesIO()
        pdf_writer.write(part_pdf)
        part_pdf.seek(0)
        parts.append(part_pdf)

    logging.info(f"Found {len(parts)} parts!")
    return parts


def unstructured_partitioning(file, filename):
    pdf_reader = PdfReader(file)

    if len(pdf_reader.pages) > 25:
        file.seek(0)
        parts = split_pdf(file)
        elements = []
        for part in parts:
            logging.info("Partitioning with unstructured...")
            part_elements = partition_via_api(
                metadata_filename="placeholder.pdf",  # this param does nothing
                file=part,
                strategy="hi_res",
                pdf_infer_table_structure=True,
                skip_infer_table_types="[]",
                api_url=api_url,
                api_key=api_key,
            )
            elements.extend(part_elements)
    else:
        logging.info("Partitioning with unstructured...")
        file.seek(0)
        elements = partition_via_api(
            metadata_filename=filename,
            file=file,
            strategy="hi_res",
            pdf_infer_table_structure=True,
            skip_infer_table_types="[]",
            api_url=api_url,
            api_key=api_key,
        )

    elements_json = json.loads(elements_to_json(elements))

    return elements_json


def unstructured_filtering(elements):
    logging.info("Filtering with unstructured...")
    filtered_elements = []

    for element in elements:
        if element["type"] in ["Title", "NarrativeText", "Table", "ListItem", "Image"]:
            filtered_elements.append(element)

    return filtered_elements


def unstructured_cleaning(json_data):
    logging.info("Cleaning with unstructured...")
    cleaned_data = []

    for item in json_data:
        if item["type"] == "Table":
            cleaned_text = clean_extra_whitespace(
                clean_bullets(item["metadata"].get("text_as_html", ""))
            )
            if len(cleaned_text) > 0:
                item["metadata"]["text_as_html"] = cleaned_text
                cleaned_data.append(item)
            else:
                logging.info(f"Removed item of type 'Table' due to empty text: {item}")
        else:
            cleaned_text = clean_extra_whitespace(clean_bullets(item.get("text", "")))
            if len(cleaned_text) > 0:
                item["text"] = cleaned_text
                cleaned_data.append(item)
            else:
                logging.info(
                    f"Removed item of type '{item['type']}' due to empty text: {item}"
                )

    return cleaned_data


def unstructured_convert_pdf_to_json_array(file, filename):
    partitioned_elements = unstructured_partitioning(file, filename)
    filtered_elements = unstructured_filtering(partitioned_elements)
    cleaned_elements = unstructured_cleaning(filtered_elements)

    return cleaned_elements
