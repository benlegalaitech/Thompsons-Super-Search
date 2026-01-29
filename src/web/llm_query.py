"""LLM-powered query parsing for smart search."""

import os
import re
import json
import hashlib
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from functools import lru_cache

from openai import OpenAI

# Configuration from environment
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4o-mini-2024-07-18')
SMART_SEARCH_TIMEOUT = int(os.environ.get('SMART_SEARCH_TIMEOUT', '30'))
SMART_SEARCH_CACHE_TTL = int(os.environ.get('SMART_SEARCH_CACHE_TTL', '600'))


class LLMError(Exception):
    """Base exception for LLM errors."""
    pass


class LLMTimeoutError(LLMError):
    """LLM request timed out."""
    pass


class LLMValidationError(LLMError):
    """LLM output failed validation."""
    pass


@dataclass
class DateRange:
    """Structured date range for filtering."""
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    range_type: str = "between"  # 'between', 'before', 'after', 'exact', 'none'

    def has_constraint(self) -> bool:
        """Check if this date range has any constraint."""
        return self.range_type != "none" and (self.start_year is not None or self.end_year is not None)

    def to_dict(self):
        return {
            'start_year': self.start_year,
            'end_year': self.end_year,
            'range_type': self.range_type
        }

    def describe(self) -> str:
        """Return human-readable description of the date constraint."""
        if not self.has_constraint():
            return ""
        if self.range_type == "between" and self.start_year and self.end_year:
            return f"between {self.start_year} and {self.end_year}"
        if self.range_type == "after" and self.start_year:
            return f"after {self.start_year}"
        if self.range_type == "before" and self.end_year:
            return f"before {self.end_year}"
        if self.range_type == "exact" and self.start_year:
            return f"in {self.start_year}"
        return ""


@dataclass
class QueryAnalysis:
    """Query analysis with intent classification for smart search."""

    # Intent classification
    intent: str = "finding"  # 'extraction', 'finding', 'specific'
    extraction_target: Optional[str] = None  # For extraction: what to extract (e.g., "company names")

    # Search strategy
    search_terms: List[str] = field(default_factory=list)  # Broad terms to find relevant docs
    search_strategy: str = "focused"  # 'broad', 'focused', 'exhaustive'

    # For backward compatibility - also include QueryPlan fields
    required_terms: List[str] = field(default_factory=list)
    optional_terms: List[str] = field(default_factory=list)
    person_names: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)
    date_hints: List[str] = field(default_factory=list)

    # Structured date range for filtering
    date_range: Optional[DateRange] = None

    # Metadata
    interpretation: str = ""
    confidence: float = 0.8
    schema_version: str = "v2"

    def to_dict(self):
        d = asdict(self)
        # Convert DateRange to dict if present
        if self.date_range:
            d['date_range'] = self.date_range.to_dict()
        return d

    def is_extraction_query(self) -> bool:
        """Check if this is an extraction query that needs post-search LLM processing."""
        return self.intent == "extraction" and self.extraction_target is not None

    def has_date_constraint(self) -> bool:
        """Check if there's a date range constraint."""
        return self.date_range is not None and self.date_range.has_constraint()


@dataclass
class QueryPlan:
    """Validated, typed query plan from LLM output."""

    # Required fields
    required_terms: List[str] = field(default_factory=list)
    interpretation: str = ""

    # Optional fields with caps
    optional_terms: List[str] = field(default_factory=list)
    person_names: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)
    date_hints: List[str] = field(default_factory=list)

    # Metadata
    confidence: float = 0.8
    schema_version: str = "v1"

    def to_dict(self):
        return asdict(self)


def sanitize_term(term: str) -> str:
    """Sanitize a search term - alphanumeric, spaces, hyphens only."""
    if not term:
        return ""
    # Keep alphanumeric, spaces, and hyphens
    cleaned = re.sub(r'[^a-zA-Z0-9\s\-]', '', term)
    return cleaned.strip().lower()


def validate_query_plan(plan_dict: dict) -> QueryPlan:
    """Validate and sanitize LLM output into a QueryPlan."""

    # Extract and sanitize required terms
    required = plan_dict.get('required_terms', [])
    if isinstance(required, str):
        required = [required]
    required = [sanitize_term(t) for t in required if sanitize_term(t)]

    # Extract and sanitize optional terms (synonyms)
    optional = plan_dict.get('optional_terms', [])
    if isinstance(optional, str):
        optional = [optional]
    optional = [sanitize_term(t) for t in optional if sanitize_term(t)]
    # Cap at 15 total synonyms
    optional = optional[:15]

    # Extract person names (cap at 3)
    person_names = plan_dict.get('person_names', [])
    if isinstance(person_names, str):
        person_names = [person_names]
    person_names = [sanitize_term(n) for n in person_names if sanitize_term(n)][:3]

    # Extract locations (cap at 3)
    locations = plan_dict.get('locations', [])
    if isinstance(locations, str):
        locations = [locations]
    locations = [sanitize_term(loc) for loc in locations if sanitize_term(loc)][:3]

    # If no required terms but we have person names or locations, use those as required terms
    # This handles queries like "Ben" or "Clydeside" where only entity is specified
    if not required:
        if person_names:
            required = person_names.copy()
        elif locations:
            required = locations.copy()

    if not required:
        raise LLMValidationError("No valid required terms extracted from query")

    # Extract date hints (cap at 2)
    date_hints = plan_dict.get('date_hints', [])
    if isinstance(date_hints, str):
        date_hints = [date_hints]
    date_hints = [str(d).strip()[:20] for d in date_hints if d][:2]

    # Total term cap (20 max)
    total_terms = len(required) + len(optional) + len(person_names) + len(locations)
    if total_terms > 20:
        # Trim optional terms to fit
        max_optional = max(0, 20 - len(required) - len(person_names) - len(locations))
        optional = optional[:max_optional]

    # Confidence
    confidence = float(plan_dict.get('confidence', 0.8))
    confidence = max(0.0, min(1.0, confidence))

    # Interpretation (cap at 200 chars)
    interpretation = str(plan_dict.get('interpretation', ''))[:200]

    return QueryPlan(
        required_terms=required,
        optional_terms=optional,
        person_names=person_names,
        locations=locations,
        date_hints=date_hints,
        confidence=confidence,
        interpretation=interpretation
    )


SYSTEM_PROMPT = """You are a legal document search assistant. Your job is to transform natural language search queries into structured search plans.

The document collection contains legal case documents including:
- Medical/industrial documents: asbestos exposure, mesothelioma, lung disease, occupational health
- Automotive/emissions documents: diesel emissions, EOBD codes, vehicle testing, exhaust systems
- General legal: contracts, correspondence, memos, reports

Given a user's search query, extract the following information and return it as JSON:

1. **required_terms**: Keywords that MUST appear in matching documents. These are the core concepts the user is searching for. Include the most specific terms.

2. **optional_terms**: Related terms, synonyms, or alternative phrasings that might also appear. These help find more relevant results. For example:
   - "asbestos" → also include "chrysotile", "mesothelioma", "asbestosis"
   - "diesel emissions" → also include "exhaust", "DPF", "particulate", "NOx"
   - "contract" → also include "agreement", "terms"

3. **person_names**: Any person names mentioned in the query. Extract first names, last names, or full names.

4. **locations**: Any place names, company names, or locations mentioned.

5. **date_hints**: Any temporal references like "1990s", "before 2000", "recent".

6. **interpretation**: A brief (1-2 sentence) plain English explanation of what the user is looking for.

7. **confidence**: A score from 0.0 to 1.0 indicating how confident you are in understanding the query. Use lower scores (< 0.7) for vague or ambiguous queries.

Return ONLY valid JSON with these fields. Example:

{
    "required_terms": ["asbestos", "exposure"],
    "optional_terms": ["mesothelioma", "chrysotile", "lung disease"],
    "person_names": ["john smith"],
    "locations": ["clydeside"],
    "date_hints": [],
    "interpretation": "Documents about asbestos exposure, specifically mentioning a person named John Smith, related to the Clydeside area.",
    "confidence": 0.9
}"""


INTENT_CLASSIFICATION_PROMPT = """You are a legal document search assistant. Your job is to understand what the user wants and create an intelligent search strategy.

IMPORTANT: First, classify the user's INTENT:

1. **extraction** - User wants to LIST or EXTRACT entities from documents
   Examples: "list all companies", "what people are mentioned", "show me all dates", "what vehicles are referenced"

2. **finding** - User wants to FIND documents about a topic
   Examples: "find documents about asbestos", "show me reports on diesel emissions", "get contracts from 2020"

3. **specific** - User wants a SPECIFIC item
   Examples: "find John Smith's testimony", "get the Ford contract", "show document ABC-123"

For EXTRACTION queries:
- Identify WHAT they want to extract (company names, people, dates, vehicles, medical conditions, etc.)
- Generate BROAD search terms to find documents that might contain those entities
- The system will then read those documents and extract the specific entities

For FINDING queries:
- Generate focused search terms for the topic
- Include synonyms and related terms

IMPORTANT - Date Range Extraction:
If the user specifies a date range (e.g., "between 1970 and 1980", "before 2000", "after 1990", "in 1985"), extract it as a structured date_range object:
- "between X and Y" → {"start_year": X, "end_year": Y, "range_type": "between"}
- "before X" → {"start_year": null, "end_year": X, "range_type": "before"}
- "after X" → {"start_year": X, "end_year": null, "range_type": "after"}
- "in X" or "during X" → {"start_year": X, "end_year": X, "range_type": "exact"}
- No date mentioned → {"start_year": null, "end_year": null, "range_type": "none"}

Return JSON with:
{
    "intent": "extraction" | "finding" | "specific",
    "extraction_target": "company names" (only if intent is extraction - describe what to extract),
    "search_terms": ["term1", "term2", ...],
    "search_strategy": "broad" | "focused" | "exhaustive",
    "required_terms": ["term1", ...],
    "optional_terms": ["term1", ...],
    "person_names": [],
    "locations": [],
    "date_hints": [],
    "date_range": {"start_year": null, "end_year": null, "range_type": "none"},
    "interpretation": "Plain English explanation of what user wants",
    "confidence": 0.0-1.0
}

Example for "list all welders employed by Ford between 1970 and 1980":
{
    "intent": "extraction",
    "extraction_target": "welder names",
    "search_terms": ["welder", "welding", "employed", "ford", "employee"],
    "search_strategy": "broad",
    "required_terms": ["welder", "ford"],
    "optional_terms": ["welding", "employed", "employee", "worked"],
    "person_names": [],
    "locations": [],
    "date_hints": ["1970", "1980"],
    "date_range": {"start_year": 1970, "end_year": 1980, "range_type": "between"},
    "interpretation": "User wants to extract names of all welders who worked at Ford between 1970 and 1980",
    "confidence": 0.9
}

Example for "list all companies mentioned":
{
    "intent": "extraction",
    "extraction_target": "company names",
    "search_terms": ["company", "ltd", "limited", "inc", "corporation", "plc", "llc", "firm", "business", "organization"],
    "search_strategy": "broad",
    "required_terms": [],
    "optional_terms": ["company", "ltd", "limited", "inc", "corporation", "plc", "llc"],
    "person_names": [],
    "locations": [],
    "date_hints": [],
    "date_range": {"start_year": null, "end_year": null, "range_type": "none"},
    "interpretation": "User wants to extract and list all company names mentioned across the documents",
    "confidence": 0.95
}

Example for "find documents about asbestos exposure":
{
    "intent": "finding",
    "extraction_target": null,
    "search_terms": ["asbestos", "exposure", "mesothelioma", "asbestosis"],
    "search_strategy": "focused",
    "required_terms": ["asbestos"],
    "optional_terms": ["exposure", "mesothelioma", "asbestosis", "chrysotile", "lung disease"],
    "person_names": [],
    "locations": [],
    "date_hints": [],
    "date_range": {"start_year": null, "end_year": null, "range_type": "none"},
    "interpretation": "User wants documents discussing asbestos exposure and related health conditions",
    "confidence": 0.9
}"""


# Simple in-memory cache for query plans
_query_cache = {}
_cache_timestamps = {}


def _get_cache_key(project_id: str, query: str) -> str:
    """Generate cache key for a query."""
    return hashlib.md5(f"{project_id}:{query.lower().strip()}".encode()).hexdigest()


def _get_cached_plan(project_id: str, query: str) -> Optional[QueryPlan]:
    """Get cached query plan if still valid."""
    key = _get_cache_key(project_id, query)
    if key in _query_cache:
        timestamp = _cache_timestamps.get(key, 0)
        if time.time() - timestamp < SMART_SEARCH_CACHE_TTL:
            return _query_cache[key]
        else:
            # Expired - remove from cache
            del _query_cache[key]
            del _cache_timestamps[key]
    return None


def _cache_plan(project_id: str, query: str, plan):
    """Cache a query plan or analysis."""
    key = _get_cache_key(project_id, query)
    _query_cache[key] = plan
    _cache_timestamps[key] = time.time()

    # Simple cache size limit (remove oldest if over 1000)
    if len(_query_cache) > 1000:
        oldest_key = min(_cache_timestamps, key=_cache_timestamps.get)
        del _query_cache[oldest_key]
        del _cache_timestamps[oldest_key]


def validate_query_analysis(plan_dict: dict) -> QueryAnalysis:
    """Validate and sanitize LLM output into a QueryAnalysis."""
    import sys

    # Extract intent
    intent = plan_dict.get('intent', 'finding')
    if intent not in ('extraction', 'finding', 'specific'):
        intent = 'finding'

    # Extract extraction target
    extraction_target = plan_dict.get('extraction_target')
    if extraction_target:
        extraction_target = str(extraction_target)[:100]

    # Extract search strategy
    search_strategy = plan_dict.get('search_strategy', 'focused')
    if search_strategy not in ('broad', 'focused', 'exhaustive'):
        search_strategy = 'focused'

    # Extract and sanitize search terms
    search_terms = plan_dict.get('search_terms', [])
    if isinstance(search_terms, str):
        search_terms = [search_terms]
    search_terms = [sanitize_term(t) for t in search_terms if sanitize_term(t)][:20]

    # Extract and sanitize required terms
    required = plan_dict.get('required_terms', [])
    if isinstance(required, str):
        required = [required]
    required = [sanitize_term(t) for t in required if sanitize_term(t)]

    # Extract and sanitize optional terms
    optional = plan_dict.get('optional_terms', [])
    if isinstance(optional, str):
        optional = [optional]
    optional = [sanitize_term(t) for t in optional if sanitize_term(t)][:15]

    # Extract person names
    person_names = plan_dict.get('person_names', [])
    if isinstance(person_names, str):
        person_names = [person_names]
    person_names = [sanitize_term(n) for n in person_names if sanitize_term(n)][:5]

    # Extract locations
    locations = plan_dict.get('locations', [])
    if isinstance(locations, str):
        locations = [locations]
    locations = [sanitize_term(loc) for loc in locations if sanitize_term(loc)][:5]

    # Extract date hints
    date_hints = plan_dict.get('date_hints', [])
    if isinstance(date_hints, str):
        date_hints = [date_hints]
    date_hints = [str(d).strip()[:20] for d in date_hints if d][:3]

    # Extract structured date range
    date_range = None
    date_range_dict = plan_dict.get('date_range', {})
    if date_range_dict and isinstance(date_range_dict, dict):
        range_type = date_range_dict.get('range_type', 'none')
        if range_type in ('between', 'before', 'after', 'exact'):
            start_year = date_range_dict.get('start_year')
            end_year = date_range_dict.get('end_year')
            # Validate years are reasonable (1900-2100)
            if start_year is not None:
                try:
                    start_year = int(start_year)
                    if not (1900 <= start_year <= 2100):
                        start_year = None
                except (ValueError, TypeError):
                    start_year = None
            if end_year is not None:
                try:
                    end_year = int(end_year)
                    if not (1900 <= end_year <= 2100):
                        end_year = None
                except (ValueError, TypeError):
                    end_year = None
            date_range = DateRange(
                start_year=start_year,
                end_year=end_year,
                range_type=range_type
            )
            print(f"[LLM-QUERY] Parsed date_range: {date_range.describe()}", file=sys.stderr)

    # For extraction queries, ensure we have search terms
    if intent == 'extraction' and not search_terms:
        # Fall back to optional terms or generic terms
        search_terms = optional[:10] if optional else ['document', 'report', 'file']

    # For finding/specific queries with no required terms, use search terms
    if intent in ('finding', 'specific') and not required:
        if person_names:
            required = person_names.copy()
        elif locations:
            required = locations.copy()
        elif search_terms:
            required = search_terms[:3]

    # Confidence
    confidence = float(plan_dict.get('confidence', 0.8))
    confidence = max(0.0, min(1.0, confidence))

    # Interpretation
    interpretation = str(plan_dict.get('interpretation', ''))[:300]

    print(f"[LLM-QUERY] Validated analysis: intent={intent}, extraction_target={extraction_target}, "
          f"search_terms={search_terms[:5]}, required={required[:3]}", file=sys.stderr)

    return QueryAnalysis(
        intent=intent,
        extraction_target=extraction_target,
        search_terms=search_terms,
        search_strategy=search_strategy,
        required_terms=required,
        optional_terms=optional,
        person_names=person_names,
        locations=locations,
        date_hints=date_hints,
        date_range=date_range,
        interpretation=interpretation,
        confidence=confidence
    )


def analyze_query(query: str, project_id: str = "", project_description: str = "") -> QueryAnalysis:
    """
    Analyze a natural language query to determine intent and search strategy.

    This is the new entry point for smart search that supports extraction queries.

    Args:
        query: The user's natural language search query
        project_id: Project identifier for caching
        project_description: Optional description of the project/document collection

    Returns:
        QueryAnalysis with intent classification and search strategy

    Raises:
        LLMError: If the LLM call fails
        LLMValidationError: If the LLM output can't be validated
    """
    import sys
    print(f"[LLM-QUERY] analyze_query called: query='{query}'", file=sys.stderr)

    # Check cache first (use different cache key prefix for analysis)
    cache_key = f"analysis:{project_id}:{query.lower().strip()}"
    cache_key_hash = hashlib.md5(cache_key.encode()).hexdigest()
    if cache_key_hash in _query_cache:
        timestamp = _cache_timestamps.get(cache_key_hash, 0)
        if time.time() - timestamp < SMART_SEARCH_CACHE_TTL:
            print(f"[LLM-QUERY] Analysis cache hit!", file=sys.stderr)
            return _query_cache[cache_key_hash]

    if not OPENAI_API_KEY:
        print(f"[LLM-QUERY] ERROR: No API key configured!", file=sys.stderr)
        raise LLMError("OpenAI API key not configured")

    # Build the prompt
    user_message = f"Search query: {query}"
    if project_description:
        user_message = f"Document collection: {project_description}\n\n{user_message}"

    # Call OpenAI with retries
    client = OpenAI(api_key=OPENAI_API_KEY, timeout=SMART_SEARCH_TIMEOUT)

    max_retries = 2
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            print(f"[LLM-QUERY] Calling OpenAI for intent classification (attempt {attempt + 1})...", file=sys.stderr)
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": INTENT_CLASSIFICATION_PROMPT},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.3,
                max_tokens=800,
                response_format={"type": "json_object"}
            )

            # Parse the response
            content = response.choices[0].message.content
            print(f"[LLM-QUERY] Raw LLM response: {content[:500]}...", file=sys.stderr)
            plan_dict = json.loads(content)

            # Validate and create QueryAnalysis
            analysis = validate_query_analysis(plan_dict)

            # Cache the result
            _query_cache[cache_key_hash] = analysis
            _cache_timestamps[cache_key_hash] = time.time()

            return analysis

        except json.JSONDecodeError as e:
            print(f"[LLM-QUERY] JSON decode error: {e}", file=sys.stderr)
            last_error = LLMValidationError(f"Invalid JSON from LLM: {e}")
        except LLMValidationError as e:
            print(f"[LLM-QUERY] Validation error: {e}", file=sys.stderr)
            last_error = e
        except Exception as e:
            print(f"[LLM-QUERY] Exception: {type(e).__name__}: {e}", file=sys.stderr)
            if "timeout" in str(e).lower():
                last_error = LLMTimeoutError("Search is taking longer than expected. Please try again.")
            else:
                last_error = LLMError(f"LLM error: {e}")

        # Wait before retry
        if attempt < max_retries:
            print(f"[LLM-QUERY] Retrying (attempt {attempt + 2})...", file=sys.stderr)
            time.sleep(2.0 * (attempt + 1))

    print(f"[LLM-QUERY] All retries exhausted, raising: {last_error}", file=sys.stderr)
    raise last_error


def parse_query_with_llm(query: str, project_id: str = "", project_description: str = "") -> QueryPlan:
    """
    Parse a natural language query using the LLM.

    Args:
        query: The user's natural language search query
        project_id: Project identifier for caching
        project_description: Optional description of the project/document collection

    Returns:
        QueryPlan with extracted search terms and metadata

    Raises:
        LLMError: If the LLM call fails
        LLMValidationError: If the LLM output can't be validated
    """
    import sys
    print(f"[LLM-QUERY] parse_query_with_llm called: query='{query}'", file=sys.stderr)

    # Check cache first
    cached = _get_cached_plan(project_id, query)
    if cached:
        print(f"[LLM-QUERY] Cache hit!", file=sys.stderr)
        return cached

    if not OPENAI_API_KEY:
        print(f"[LLM-QUERY] ERROR: No API key configured!", file=sys.stderr)
        raise LLMError("OpenAI API key not configured")

    print(f"[LLM-QUERY] API key present (length={len(OPENAI_API_KEY)}), model={OPENAI_MODEL}", file=sys.stderr)

    # Build the prompt
    user_message = f"Search query: {query}"
    if project_description:
        user_message = f"Document collection: {project_description}\n\n{user_message}"

    # Call OpenAI with retries
    client = OpenAI(api_key=OPENAI_API_KEY, timeout=SMART_SEARCH_TIMEOUT)

    max_retries = 2
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.3,  # Low temperature for more consistent output
                max_tokens=500,
                response_format={"type": "json_object"}
            )

            # Parse the response
            content = response.choices[0].message.content
            plan_dict = json.loads(content)

            # Validate and create QueryPlan
            plan = validate_query_plan(plan_dict)

            # Cache the result
            _cache_plan(project_id, query, plan)

            return plan

        except json.JSONDecodeError as e:
            print(f"[LLM-QUERY] JSON decode error: {e}", file=sys.stderr)
            last_error = LLMValidationError(f"Invalid JSON from LLM: {e}")
        except Exception as e:
            print(f"[LLM-QUERY] Exception: {type(e).__name__}: {e}", file=sys.stderr)
            if "timeout" in str(e).lower():
                last_error = LLMTimeoutError("Search is taking longer than expected. Please try again.")
            else:
                last_error = LLMError(f"LLM error: {e}")

        # Wait before retry (exponential backoff)
        if attempt < max_retries:
            print(f"[LLM-QUERY] Retrying (attempt {attempt + 2})...", file=sys.stderr)
            time.sleep(2.0 * (attempt + 1))

    print(f"[LLM-QUERY] All retries exhausted, raising: {last_error}", file=sys.stderr)
    raise last_error


def is_smart_search_enabled() -> bool:
    """Check if smart search is enabled."""
    return os.environ.get('SMART_SEARCH_ENABLED', 'true').lower() == 'true'


def is_keyword_search_enabled() -> bool:
    """Check if keyword search mode is enabled."""
    return os.environ.get('KEYWORD_SEARCH_ENABLED', 'true').lower() == 'true'
