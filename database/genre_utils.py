"""Genre normalization utilities for Movie Picker."""

from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

# Seed data for genres table: (canonical_name, aliases, tmdb_movie_id, tmdb_tv_id)
GENRE_SEED_DATA = [
    ("боевик", "action, экшен, экшн", 28, 10759),
    ("приключения", "adventure, приключение", 12, 10759),
    ("мультфильм", "animation, анимация", 16, 16),
    ("комедия", "comedy", 35, 35),
    ("криминал", "crime, криминальный", 80, 80),
    ("документальный", "documentary, документалка", 99, 99),
    ("драма", "drama", 18, 18),
    ("семейный", "family, семья", 10751, 10751),
    ("фэнтези", "fantasy", 14, 10765),
    ("история", "history, исторический", 36, None),
    ("ужасы", "horror, хоррор", 27, None),
    ("музыка", "music, музыкальный", 10402, None),
    ("детектив", "mystery, мистика", 9648, 9648),
    ("мелодрама", "romance, романтика", 10749, None),
    ("фантастика", "science fiction, sci-fi, нф", 878, 10765),
    ("тв фильм", "tv movie", 10770, None),
    ("триллер", "thriller", 53, None),
    ("военный", "war, война", 10752, 10768),
    ("вестерн", "western", 37, 37),
    # TV-specific genres
    ("детский", "kids", None, 10762),
    ("новости", "news", None, 10763),
    ("реалити", "reality", None, 10764),
    ("мыльная опера", "soap", None, 10766),
    ("ток-шоу", "talk", None, 10767),
]

# Combined genre mappings (API may return these as single genres)
COMBINED_GENRES = {
    "боевик и приключения": ["боевик", "приключения"],
    "action & adventure": ["боевик", "приключения"],
    "нф и фэнтези": ["фантастика", "фэнтези"],
    "sci-fi & fantasy": ["фантастика", "фэнтези"],
    "война и политика": ["военный"],
    "war & politics": ["военный"],
}

# Caches for genre lookups (name/alias -> Genre.id)
_genre_cache: dict[str, int] = {}
_alias_cache: dict[str, int] = {}
_cache_initialized = False


def init_genre_cache(session: Session):
    """Initialize genre caches from database."""
    global _genre_cache, _alias_cache, _cache_initialized

    from .models import Genre

    _genre_cache.clear()
    _alias_cache.clear()

    for genre in session.query(Genre).all():
        _genre_cache[genre.name.lower()] = genre.id
        if genre.aliases:
            for alias in genre.aliases.split(', '):
                alias = alias.strip().lower()
                if alias:
                    _alias_cache[alias] = genre.id

    _cache_initialized = True


def _ensure_cache(session: Session):
    """Ensure genre cache is initialized."""
    global _cache_initialized
    if not _cache_initialized:
        init_genre_cache(session)


def normalize_genres(genre_string: str, session: Session) -> list[int]:
    """
    Parse and normalize genre string into list of Genre IDs.

    Handles:
    - Comma-separated: "драма, комедия"
    - Combined with "и": "боевик и приключения"
    - English and Russian names
    - Unknown genres are ignored

    Returns list of canonical Genre IDs.
    """
    if not genre_string:
        return []

    _ensure_cache(session)

    genre_ids = set()

    # First, check for known combined genre patterns (full string match)
    genre_lower = genre_string.lower().strip()
    if genre_lower in COMBINED_GENRES:
        for canonical_name in COMBINED_GENRES[genre_lower]:
            if canonical_name in _genre_cache:
                genre_ids.add(_genre_cache[canonical_name])
        return list(genre_ids)

    # Split by comma first
    parts = genre_string.split(',')

    for part in parts:
        part = part.strip()
        if not part:
            continue

        part_lower = part.lower()

        # Check if this part is a known combined genre
        if part_lower in COMBINED_GENRES:
            for canonical_name in COMBINED_GENRES[part_lower]:
                if canonical_name in _genre_cache:
                    genre_ids.add(_genre_cache[canonical_name])
            continue

        # Check if this part contains " и " (and split further)
        if ' и ' in part_lower:
            subparts = part.split(' и ')
            for subpart in subparts:
                subpart = subpart.strip().lower()
                if not subpart:
                    continue
                genre_id = _lookup_genre(subpart)
                if genre_id is not None:
                    genre_ids.add(genre_id)
        else:
            # Single genre - look up directly
            genre_id = _lookup_genre(part_lower)
            if genre_id is not None:
                genre_ids.add(genre_id)

    return list(genre_ids)


def _lookup_genre(name: str) -> Optional[int]:
    """Look up genre ID by name or alias. Returns None if not found."""
    # Look up in canonical names first
    if name in _genre_cache:
        return _genre_cache[name]

    # Look up in aliases
    if name in _alias_cache:
        return _alias_cache[name]

    # Unknown genre - return None (will be ignored)
    return None


def get_genre_name_by_id(genre_id: int, session: Session) -> Optional[str]:
    """Get canonical genre name by ID."""
    from .models import Genre

    genre = session.query(Genre).get(genre_id)
    return genre.name if genre else None


def clear_cache():
    """Clear the genre cache. Useful for testing."""
    global _genre_cache, _alias_cache, _cache_initialized
    _genre_cache.clear()
    _alias_cache.clear()
    _cache_initialized = False


# =============================================================================
# Async versions
# =============================================================================

async def init_genre_cache_async(session: AsyncSession):
    """Initialize genre caches from database (async version)."""
    global _genre_cache, _alias_cache, _cache_initialized

    from .models import Genre

    _genre_cache.clear()
    _alias_cache.clear()

    result = await session.execute(select(Genre))
    for genre in result.scalars().all():
        _genre_cache[genre.name.lower()] = genre.id
        if genre.aliases:
            for alias in genre.aliases.split(', '):
                alias = alias.strip().lower()
                if alias:
                    _alias_cache[alias] = genre.id

    _cache_initialized = True


async def normalize_genres_async(genre_string: str, session: AsyncSession) -> list[int]:
    """
    Parse and normalize genre string into list of Genre IDs (async version).
    """
    if not genre_string:
        return []

    global _cache_initialized
    if not _cache_initialized:
        await init_genre_cache_async(session)

    genre_ids = set()

    # First, check for known combined genre patterns
    genre_lower = genre_string.lower().strip()
    if genre_lower in COMBINED_GENRES:
        for canonical_name in COMBINED_GENRES[genre_lower]:
            if canonical_name in _genre_cache:
                genre_ids.add(_genre_cache[canonical_name])
        return list(genre_ids)

    # Split by comma first
    parts = genre_string.split(',')

    for part in parts:
        part = part.strip()
        if not part:
            continue

        part_lower = part.lower()

        # Check if this part is a known combined genre
        if part_lower in COMBINED_GENRES:
            for canonical_name in COMBINED_GENRES[part_lower]:
                if canonical_name in _genre_cache:
                    genre_ids.add(_genre_cache[canonical_name])
            continue

        # Check if this part contains " и " (and split further)
        if ' и ' in part_lower:
            subparts = part.split(' и ')
            for subpart in subparts:
                subpart = subpart.strip().lower()
                if not subpart:
                    continue
                genre_id = _lookup_genre(subpart)
                if genre_id is not None:
                    genre_ids.add(genre_id)
        else:
            genre_id = _lookup_genre(part_lower)
            if genre_id is not None:
                genre_ids.add(genre_id)

    return list(genre_ids)
