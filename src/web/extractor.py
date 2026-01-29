"""LLM-powered entity extraction from document text."""

import os
import json
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from collections import defaultdict

from openai import OpenAI

# Configuration
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4o-mini-2024-07-18')
EXTRACTION_TIMEOUT = int(os.environ.get('EXTRACTION_TIMEOUT', '60'))

# Token limits - conservative to leave room for response
MAX_TOKENS_PER_BATCH = 80000  # ~35 pages of text
CHARS_PER_TOKEN = 4  # Rough estimate
MAX_CHARS_PER_BATCH = MAX_TOKENS_PER_BATCH * CHARS_PER_TOKEN


class ExtractionError(Exception):
    """Error during extraction."""
    pass


@dataclass
class ExtractedEntity:
    """A single extracted entity with its source."""
    value: str
    document: str
    page: int
    context: str = ""


@dataclass
class ExtractionResult:
    """Aggregated extraction results."""
    query: str
    extraction_target: str
    entities: List[Dict[str, Any]] = field(default_factory=list)
    total_unique: int = 0
    total_mentions: int = 0
    documents_searched: int = 0
    pages_analyzed: int = 0
    extraction_time_ms: int = 0

    def to_dict(self):
        return asdict(self)


EXTRACTION_PROMPT_TEMPLATE = """You are extracting information from legal documents.

User's original request: "{query}"
Extraction target: {extraction_target}
{date_constraint}
Analyze the following document text and extract all {extraction_target}.

IMPORTANT INSTRUCTIONS:
1. Extract EVERY instance of {extraction_target} you find
2. Include the exact text as it appears in the document
3. Provide brief context (the sentence or phrase where it appears)
4. Include the document name and page number for each extraction
5. Be thorough - don't miss any mentions
{date_instruction}
Documents to analyze:
---
{document_text}
---

Return your extractions as JSON:
{{
    "extractions": [
        {{"value": "Example Entity", "document": "filename.pdf", "page": 1, "context": "...brief context where it appears..."}},
        ...
    ],
    "notes": "Any relevant observations about the extractions"
}}

Extract ALL {extraction_target}. Be comprehensive."""


EXTRACTION_PROMPT_TEMPLATE_WITH_DATES = """You are extracting information from legal documents.

User's original request: "{query}"
Extraction target: {extraction_target}
Date constraint: Only extract entities associated with dates {date_constraint}

Analyze the following document text and extract all {extraction_target}.

CRITICAL DATE FILTERING INSTRUCTIONS:
1. ONLY extract entities that are clearly associated with dates {date_constraint}
2. If a person/entity is mentioned but their associated date is OUTSIDE this range, DO NOT include them
3. Look for employment dates, document dates, event dates, or any temporal references
4. If no date is mentioned for an entity, DO NOT include it (err on the side of caution)
5. Include the associated date in the context field

EXTRACTION INSTRUCTIONS:
1. Extract EVERY instance of {extraction_target} that matches the date criteria
2. Include the exact text as it appears in the document
3. Provide brief context INCLUDING THE DATE that qualifies this extraction
4. Include the document name and page number for each extraction
5. Be thorough but STRICT about the date constraint

Documents to analyze:
---
{document_text}
---

Return your extractions as JSON:
{{
    "extractions": [
        {{"value": "Example Entity", "document": "filename.pdf", "page": 1, "context": "...brief context INCLUDING THE DATE that qualifies this..."}},
        ...
    ],
    "notes": "Any relevant observations about the extractions and date filtering applied"
}}

Remember: ONLY include {extraction_target} associated with dates {date_constraint}."""


def create_document_batches(search_results: List[Dict], max_chars: int = MAX_CHARS_PER_BATCH) -> List[List[Dict]]:
    """
    Group search results into batches that fit within token limits.

    Args:
        search_results: List of search result dicts with 'filename', 'page', 'context' or 'text'
        max_chars: Maximum characters per batch

    Returns:
        List of batches, where each batch is a list of results
    """
    batches = []
    current_batch = []
    current_chars = 0

    for result in search_results:
        # Get the text content
        text = result.get('text', result.get('context', ''))
        text_chars = len(text)

        # If single result exceeds limit, truncate it
        if text_chars > max_chars:
            text = text[:max_chars - 1000]  # Leave room for metadata
            text_chars = len(text)
            result = result.copy()
            result['text'] = text

        # If adding this result would exceed limit, start new batch
        if current_chars + text_chars > max_chars and current_batch:
            batches.append(current_batch)
            current_batch = []
            current_chars = 0

        current_batch.append(result)
        current_chars += text_chars

    # Don't forget the last batch
    if current_batch:
        batches.append(current_batch)

    return batches


def format_batch_for_extraction(batch: List[Dict]) -> str:
    """
    Format a batch of search results as text for the LLM.

    Args:
        batch: List of search result dicts

    Returns:
        Formatted text string
    """
    parts = []

    for result in batch:
        filename = result.get('filename', 'Unknown')
        page = result.get('page', 0)
        text = result.get('text', result.get('context', ''))

        parts.append(f"[Document: {filename}, Page {page}]\n{text}\n")

    return "\n---\n".join(parts)


def extract_from_batch(
    batch: List[Dict],
    query: str,
    extraction_target: str,
    client: OpenAI,
    date_range: Optional[Dict] = None
) -> List[ExtractedEntity]:
    """
    Extract entities from a single batch of documents.

    Args:
        batch: List of search result dicts
        query: Original user query
        extraction_target: What to extract (e.g., "company names")
        client: OpenAI client
        date_range: Optional date range constraint dict with start_year, end_year, range_type

    Returns:
        List of ExtractedEntity objects
    """
    # Format the batch
    document_text = format_batch_for_extraction(batch)

    # Build the prompt - use date-aware template if date range provided
    if date_range and date_range.get('range_type', 'none') != 'none':
        # Build date constraint description
        range_type = date_range.get('range_type', 'none')
        start_year = date_range.get('start_year')
        end_year = date_range.get('end_year')

        if range_type == 'between' and start_year and end_year:
            date_constraint = f"between {start_year} and {end_year}"
        elif range_type == 'after' and start_year:
            date_constraint = f"after {start_year}"
        elif range_type == 'before' and end_year:
            date_constraint = f"before {end_year}"
        elif range_type == 'exact' and start_year:
            date_constraint = f"in {start_year}"
        else:
            date_constraint = ""

        if date_constraint:
            prompt = EXTRACTION_PROMPT_TEMPLATE_WITH_DATES.format(
                query=query,
                extraction_target=extraction_target,
                date_constraint=date_constraint,
                document_text=document_text
            )
            print(f"[EXTRACTOR] Using date-constrained extraction: {date_constraint}", file=sys.stderr)
        else:
            prompt = EXTRACTION_PROMPT_TEMPLATE.format(
                query=query,
                extraction_target=extraction_target,
                date_constraint="",
                date_instruction="",
                document_text=document_text
            )
    else:
        prompt = EXTRACTION_PROMPT_TEMPLATE.format(
            query=query,
            extraction_target=extraction_target,
            date_constraint="",
            date_instruction="",
            document_text=document_text
        )

    print(f"[EXTRACTOR] Sending batch of {len(batch)} results to LLM ({len(document_text)} chars)...", file=sys.stderr)

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,  # Low temperature for accuracy
            max_tokens=4000,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content
        result = json.loads(content)

        extractions = result.get('extractions', [])
        print(f"[EXTRACTOR] Extracted {len(extractions)} entities from batch", file=sys.stderr)

        entities = []
        for ext in extractions:
            entities.append(ExtractedEntity(
                value=str(ext.get('value', '')).strip(),
                document=str(ext.get('document', '')),
                page=int(ext.get('page', 0)),
                context=str(ext.get('context', ''))[:200]
            ))

        return entities

    except json.JSONDecodeError as e:
        print(f"[EXTRACTOR] JSON decode error: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[EXTRACTOR] Error in batch extraction: {e}", file=sys.stderr)
        return []


def aggregate_extractions(entities: List[ExtractedEntity]) -> List[Dict[str, Any]]:
    """
    Aggregate and deduplicate extracted entities.

    Args:
        entities: List of ExtractedEntity objects

    Returns:
        List of aggregated entity dicts with mention counts
    """
    # Group by normalized value
    grouped = defaultdict(lambda: {
        'value': '',
        'mentions': 0,
        'documents': set(),
        'pages': [],
        'contexts': []
    })

    for entity in entities:
        if not entity.value:
            continue

        # Normalize for grouping (case-insensitive)
        key = entity.value.lower().strip()

        group = grouped[key]
        if not group['value']:
            group['value'] = entity.value  # Keep first casing

        group['mentions'] += 1
        group['documents'].add(entity.document)
        group['pages'].append({'document': entity.document, 'page': entity.page})
        if entity.context and len(group['contexts']) < 3:
            group['contexts'].append(entity.context)

    # Convert to list and sort by mentions (most frequent first)
    result = []
    for key, group in grouped.items():
        result.append({
            'value': group['value'],
            'mentions': group['mentions'],
            'documents': list(group['documents']),
            'first_context': group['contexts'][0] if group['contexts'] else '',
            'sample_pages': group['pages'][:5]  # First 5 page references
        })

    result.sort(key=lambda x: x['mentions'], reverse=True)

    return result


def extract_entities(
    query: str,
    extraction_target: str,
    search_results: List[Dict],
    full_texts: Optional[Dict[str, Dict]] = None,
    date_range: Optional[Dict] = None
) -> ExtractionResult:
    """
    Extract entities from search results using LLM.

    This is the main entry point for the extraction engine.

    Args:
        query: Original user query
        extraction_target: What to extract (e.g., "company names", "people", "dates")
        search_results: List of search result dicts from the search engine
        full_texts: Optional dict mapping filename to full document text for richer extraction
        date_range: Optional date range constraint dict with start_year, end_year, range_type

    Returns:
        ExtractionResult with aggregated entities
    """
    start_time = time.time()

    date_info = ""
    if date_range and date_range.get('range_type', 'none') != 'none':
        range_type = date_range.get('range_type')
        start = date_range.get('start_year')
        end = date_range.get('end_year')
        if range_type == 'between' and start and end:
            date_info = f" (filtering: {start}-{end})"
        elif range_type == 'after' and start:
            date_info = f" (filtering: after {start})"
        elif range_type == 'before' and end:
            date_info = f" (filtering: before {end})"

    print(f"[EXTRACTOR] Starting extraction: target='{extraction_target}', results={len(search_results)}{date_info}", file=sys.stderr)

    if not OPENAI_API_KEY:
        raise ExtractionError("OpenAI API key not configured")

    if not search_results:
        return ExtractionResult(
            query=query,
            extraction_target=extraction_target,
            entities=[],
            total_unique=0,
            total_mentions=0,
            documents_searched=0,
            pages_analyzed=0,
            extraction_time_ms=0
        )

    # If we have full texts, enrich the search results
    if full_texts:
        enriched_results = []
        for result in search_results:
            filename = result.get('filename', '')
            if filename in full_texts:
                result = result.copy()
                page_num = result.get('page', 0)
                doc_data = full_texts[filename]
                # Try to get full page text
                pages = doc_data.get('pages', [])
                for page in pages:
                    if page.get('page_num') == page_num:
                        result['text'] = page.get('text', result.get('context', ''))
                        break
            enriched_results.append(result)
        search_results = enriched_results

    # Create batches
    batches = create_document_batches(search_results)
    print(f"[EXTRACTOR] Created {len(batches)} batches for processing", file=sys.stderr)

    # Process each batch
    client = OpenAI(api_key=OPENAI_API_KEY, timeout=EXTRACTION_TIMEOUT)
    all_entities = []

    for i, batch in enumerate(batches):
        print(f"[EXTRACTOR] Processing batch {i + 1}/{len(batches)}...", file=sys.stderr)
        entities = extract_from_batch(batch, query, extraction_target, client, date_range=date_range)
        all_entities.extend(entities)

    # Aggregate results
    aggregated = aggregate_extractions(all_entities)

    # Calculate stats
    documents_searched = len(set(r.get('filename', '') for r in search_results))
    pages_analyzed = len(search_results)
    extraction_time_ms = int((time.time() - start_time) * 1000)

    print(f"[EXTRACTOR] Extraction complete: {len(aggregated)} unique entities, "
          f"{sum(e['mentions'] for e in aggregated)} total mentions, "
          f"{extraction_time_ms}ms", file=sys.stderr)

    return ExtractionResult(
        query=query,
        extraction_target=extraction_target,
        entities=aggregated,
        total_unique=len(aggregated),
        total_mentions=sum(e['mentions'] for e in aggregated),
        documents_searched=documents_searched,
        pages_analyzed=pages_analyzed,
        extraction_time_ms=extraction_time_ms
    )
