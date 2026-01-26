import os
import json
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
from datetime import timedelta, timezone

from sqlalchemy import func, or_, select, delete
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import selectinload

from .models import (
    Base, Movie, UserRating, Genre, Director, Actor,
    MovieGenre, MovieDirector, MovieActor,
    Wishlist, RecommendationCache, utc_now
)
from .genre_utils import GENRE_SEED_DATA, init_genre_cache_async, clear_cache

_engine = None
_SessionLocal = None


async def init_db(db_path: str = "movie_picker.db"):
    """Initialize the database and create tables."""
    global _engine, _SessionLocal

    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    _engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)

    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    _SessionLocal = async_sessionmaker(bind=_engine, expire_on_commit=False)

    # Seed genres if empty and initialize cache
    async with _SessionLocal() as session:
        result = await session.execute(select(func.count(Genre.id)))
        count = result.scalar()
        if count == 0:
            await _seed_genres(session)
        await init_genre_cache_async(session)


async def _seed_genres(session: AsyncSession):
    """Seed the genres table with canonical genre data."""
    for name, aliases, tmdb_movie_id, tmdb_tv_id in GENRE_SEED_DATA:
        genre = Genre(
            name=name,
            aliases=aliases,
            tmdb_movie_id=tmdb_movie_id,
            tmdb_tv_id=tmdb_tv_id,
        )
        session.add(genre)
    await session.commit()


async def close_db():
    """Close the database engine and all connections."""
    global _engine, _SessionLocal
    if _engine:
        await _engine.dispose()
        _engine = None
        _SessionLocal = None


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session as async context manager."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    async with _SessionLocal() as session:
        yield session


# =============================================================================
# Movie CRUD
# =============================================================================

async def get_movie_by_kp_id(session: AsyncSession, kinopoisk_id: int, is_tv: bool = False) -> Optional[Movie]:
    """Get a movie/TV show by its TMDB ID and type."""
    result = await session.execute(
        select(Movie)
        .options(selectinload(Movie.genre_list), selectinload(Movie.director_list), selectinload(Movie.actor_list))
        .filter(Movie.kinopoisk_id == kinopoisk_id, Movie.is_tv == is_tv)
    )
    return result.scalar_one_or_none()


async def save_movie(session: AsyncSession, movie_data: dict) -> Movie:
    """Save or update a movie/TV show in the database.

    Handles M2M relationships for genres, directors, and actors.
    Expected keys in movie_data:
        - genres: comma-separated string "драма, комедия"
        - directors: list of dicts [{"tmdb_id": 123, "name": "Name"}, ...]
        - actors: list of dicts [{"tmdb_id": 456, "name": "Name"}, ...]
    """
    is_tv = movie_data.get("is_tv", False)
    kinopoisk_id = movie_data["kinopoisk_id"]
    movie = await get_movie_by_kp_id(session, kinopoisk_id, is_tv)

    # Extract M2M data
    genres_string = movie_data.pop("genres", None)
    directors_list = movie_data.pop("directors", None)
    actors_list = movie_data.pop("actors", None)

    try:
        if movie is None:
            movie = Movie(**movie_data)
            session.add(movie)
            await session.flush()
        else:
            for key, value in movie_data.items():
                if hasattr(movie, key) and key != "id":
                    setattr(movie, key, value)

        # Set M2M relationships
        if genres_string is not None:
            await set_movie_genres(session, movie, genres_string)
        if directors_list is not None:
            await set_movie_directors(session, movie, directors_list)
        if actors_list is not None:
            await set_movie_actors(session, movie, actors_list)

        await session.commit()
        await session.refresh(movie, ["genre_list", "director_list", "actor_list"])
    except Exception:
        await session.rollback()
        movie = await get_movie_by_kp_id(session, kinopoisk_id, is_tv)
        if movie is None:
            raise

    return movie


# =============================================================================
# M2M Setters
# =============================================================================

async def set_movie_genres(session: AsyncSession, movie: Movie, genres_string: str):
    """Set movie genres from a comma-separated string."""
    await session.execute(delete(MovieGenre).filter(MovieGenre.movie_id == movie.id))

    if not genres_string:
        return

    from .genre_utils import normalize_genres_async
    genre_ids = await normalize_genres_async(genres_string, session)
    seen_genre_ids = set()
    for genre_id in genre_ids:
        if genre_id not in seen_genre_ids:
            seen_genre_ids.add(genre_id)
            session.add(MovieGenre(movie_id=movie.id, genre_id=genre_id))


async def set_movie_directors(session: AsyncSession, movie: Movie, directors: list[dict]):
    """Set movie directors from a list of dicts with tmdb_id and name."""
    await session.execute(delete(MovieDirector).filter(MovieDirector.movie_id == movie.id))

    if not directors:
        return

    seen_tmdb_ids = set()
    for d in directors:
        tmdb_id = d.get("tmdb_id")
        name = d.get("name", "").strip()
        if not tmdb_id or not name:
            continue
        if tmdb_id in seen_tmdb_ids:
            continue
        seen_tmdb_ids.add(tmdb_id)

        director = await get_or_create_director(session, tmdb_id, name)
        session.add(MovieDirector(movie_id=movie.id, director_id=director.id))


async def set_movie_actors(session: AsyncSession, movie: Movie, actors: list[dict], limit: int = 10):
    """Set movie actors from a list of dicts with tmdb_id and name."""
    await session.execute(delete(MovieActor).filter(MovieActor.movie_id == movie.id))

    if not actors:
        return

    seen_tmdb_ids = set()
    order = 0

    for a in actors[:limit]:
        tmdb_id = a.get("tmdb_id")
        name = a.get("name", "").strip()
        if not tmdb_id or not name:
            continue
        if tmdb_id in seen_tmdb_ids:
            continue
        seen_tmdb_ids.add(tmdb_id)

        actor = await get_or_create_actor(session, tmdb_id, name)
        session.add(MovieActor(movie_id=movie.id, actor_id=actor.id, order=order))
        order += 1


# =============================================================================
# Entity Getters/Creators
# =============================================================================

async def get_or_create_director(session: AsyncSession, tmdb_id: int, name: str) -> Director:
    """Get existing director by TMDB ID or create new one."""
    result = await session.execute(select(Director).filter(Director.tmdb_id == tmdb_id))
    director = result.scalar_one_or_none()
    if director is None:
        director = Director(tmdb_id=tmdb_id, name=name)
        session.add(director)
        await session.flush()
    elif director.name != name:
        director.name = name
    return director


async def get_or_create_actor(session: AsyncSession, tmdb_id: int, name: str) -> Actor:
    """Get existing actor by TMDB ID or create new one."""
    result = await session.execute(select(Actor).filter(Actor.tmdb_id == tmdb_id))
    actor = result.scalar_one_or_none()
    if actor is None:
        actor = Actor(tmdb_id=tmdb_id, name=name)
        session.add(actor)
        await session.flush()
    elif actor.name != name:
        actor.name = name
    return actor


async def get_genre_by_id(session: AsyncSession, genre_id: int) -> Optional[Genre]:
    """Get genre by ID."""
    result = await session.execute(select(Genre).filter(Genre.id == genre_id))
    return result.scalar_one_or_none()


async def get_director_by_id(session: AsyncSession, director_id: int) -> Optional[Director]:
    """Get director by ID."""
    result = await session.execute(select(Director).filter(Director.id == director_id))
    return result.scalar_one_or_none()


async def get_actor_by_id(session: AsyncSession, actor_id: int) -> Optional[Actor]:
    """Get actor by ID."""
    result = await session.execute(select(Actor).filter(Actor.id == actor_id))
    return result.scalar_one_or_none()


# =============================================================================
# User Ratings
# =============================================================================

async def get_user_rating(session: AsyncSession, movie_id: int) -> Optional[UserRating]:
    """Get user rating for a movie."""
    result = await session.execute(
        select(UserRating)
        .options(selectinload(UserRating.movie).selectinload(Movie.genre_list))
        .filter(UserRating.movie_id == movie_id)
    )
    return result.scalar_one_or_none()


async def save_user_rating(session: AsyncSession, movie_id: int, rating: int, review: Optional[str] = None) -> UserRating:
    """Save or update user rating for a movie (fast, no entity recalc)."""
    user_rating = await get_user_rating(session, movie_id)

    if user_rating is None:
        user_rating = UserRating(movie_id=movie_id, rating=rating, review=review)
        session.add(user_rating)
    else:
        user_rating.rating = rating
        if review is not None:
            user_rating.review = review

    # Remove from wishlist if present
    result = await session.execute(select(Wishlist).filter(Wishlist.movie_id == movie_id))
    wishlist_item = result.scalar_one_or_none()
    if wishlist_item:
        await session.delete(wishlist_item)

    await session.commit()
    await session.refresh(user_rating)
    return user_rating


async def delete_user_rating(session: AsyncSession, movie_id: int) -> bool:
    """Delete user rating for a movie (fast, no entity recalc)."""
    user_rating = await get_user_rating(session, movie_id)
    if user_rating is None:
        return False

    await session.delete(user_rating)
    await session.commit()
    return True


async def update_entity_ratings_for_movie(session: AsyncSession, movie_id: int):
    """Update entity ratings for a movie (can be called in background)."""
    movie = await session.get(Movie, movie_id, options=[
        selectinload(Movie.genre_list),
        selectinload(Movie.director_list),
        selectinload(Movie.actor_list),
    ])
    if movie:
        await update_entity_ratings(session, movie)
        await session.commit()


async def get_all_user_ratings(session: AsyncSession) -> list[UserRating]:
    """Get all user ratings with their associated movies."""
    result = await session.execute(
        select(UserRating)
        .options(
            selectinload(UserRating.movie)
            .selectinload(Movie.genre_list)
        )
    )
    return list(result.unique().scalars().all())


async def get_all_user_ratings_filtered(
    session: AsyncSession,
    sort_by: str = "date_desc",
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    genres: Optional[list[str]] = None,
) -> list[UserRating]:
    """Get user ratings with sorting and filtering."""
    query = (
        select(UserRating)
        .join(Movie)
        .options(
            selectinload(UserRating.movie)
            .selectinload(Movie.genre_list)
        )
    )

    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    if max_rating is not None:
        query = query.filter(UserRating.rating <= max_rating)

    result = await session.execute(query)
    user_ratings = list(result.unique().scalars().all())

    # Apply genre filter
    if genres:
        genres_lower = [g.lower() for g in genres]
        filtered = []
        for ur in user_ratings:
            movie_genres = [g.name.lower() for g in ur.movie.genre_list]
            if any(g in movie_genres for g in genres_lower):
                filtered.append(ur)
        user_ratings = filtered

    # Apply sorting
    sort_keys = {
        "rating_desc": (lambda ur: ur.rating, True),
        "rating_asc": (lambda ur: ur.rating, False),
        "date_desc": (lambda ur: ur.updated_at or ur.created_at, True),
        "date_asc": (lambda ur: ur.updated_at or ur.created_at, False),
        "year_desc": (lambda ur: ur.movie.year or 0, True),
        "year_asc": (lambda ur: ur.movie.year or 0, False),
        "title_asc": (lambda ur: ur.movie.title.lower(), False),
        "title_desc": (lambda ur: ur.movie.title.lower(), True),
    }
    if sort_by in sort_keys:
        key_func, reverse = sort_keys[sort_by]
        user_ratings.sort(key=key_func, reverse=reverse)

    return user_ratings


async def get_rated_movies(session: AsyncSession, min_rating: Optional[int] = None) -> list[Movie]:
    """Get all movies that have user ratings."""
    query = (
        select(Movie)
        .join(UserRating)
        .options(
            selectinload(Movie.genre_list),
            selectinload(Movie.director_list),
            selectinload(Movie.actor_list),
            selectinload(Movie.user_rating),
        )
    )
    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# =============================================================================
# Entity Rating Calculations
# =============================================================================

async def update_entity_ratings(session: AsyncSession, movie: Movie):
    """Update ratings for all entities related to a movie using batch queries."""
    # Batch update genres (single query for all genre IDs)
    genre_ids = [g.id for g in movie.genre_list]
    if genre_ids:
        result = await session.execute(
            select(
                MovieGenre.genre_id,
                func.avg(UserRating.rating).label('avg_rating'),
                func.count(UserRating.id).label('rating_count')
            )
            .join(Movie, MovieGenre.movie_id == Movie.id)
            .join(UserRating, UserRating.movie_id == Movie.id)
            .filter(MovieGenre.genre_id.in_(genre_ids))
            .group_by(MovieGenre.genre_id)
        )
        genre_stats = {row.genre_id: (row.avg_rating, row.rating_count) for row in result.all()}
        for genre in movie.genre_list:
            if genre.id in genre_stats:
                genre.avg_rating, genre.rating_count = genre_stats[genre.id]
            else:
                genre.avg_rating, genre.rating_count = None, 0

    # Batch update directors
    director_ids = [d.id for d in movie.director_list]
    if director_ids:
        result = await session.execute(
            select(
                MovieDirector.director_id,
                func.avg(UserRating.rating).label('avg_rating'),
                func.count(UserRating.id).label('rating_count')
            )
            .join(Movie, MovieDirector.movie_id == Movie.id)
            .join(UserRating, UserRating.movie_id == Movie.id)
            .filter(MovieDirector.director_id.in_(director_ids))
            .group_by(MovieDirector.director_id)
        )
        director_stats = {row.director_id: (row.avg_rating, row.rating_count) for row in result.all()}
        for director in movie.director_list:
            if director.id in director_stats:
                director.avg_rating, director.rating_count = director_stats[director.id]
            else:
                director.avg_rating, director.rating_count = None, 0

    # Batch update actors
    actor_ids = [a.id for a in movie.actor_list]
    if actor_ids:
        result = await session.execute(
            select(
                MovieActor.actor_id,
                func.avg(UserRating.rating).label('avg_rating'),
                func.count(UserRating.id).label('rating_count')
            )
            .join(Movie, MovieActor.movie_id == Movie.id)
            .join(UserRating, UserRating.movie_id == Movie.id)
            .filter(MovieActor.actor_id.in_(actor_ids))
            .group_by(MovieActor.actor_id)
        )
        actor_stats = {row.actor_id: (row.avg_rating, row.rating_count) for row in result.all()}
        for actor in movie.actor_list:
            if actor.id in actor_stats:
                actor.avg_rating, actor.rating_count = actor_stats[actor.id]
            else:
                actor.avg_rating, actor.rating_count = None, 0


# =============================================================================
# Search
# =============================================================================

async def search_local_movies(session: AsyncSession, query: str) -> list[Movie]:
    """Search movies in local database using a single optimized query."""
    search_term = f"%{query.lower()}%"

    # Use subqueries to find matching movie IDs from all sources
    # Then load movies with all relationships in a single query

    # Subquery for movies matching by fields
    fields_subq = (
        select(Movie.id)
        .filter(or_(
            func.lower(Movie.title).like(search_term),
            func.lower(Movie.title_original).like(search_term),
            func.lower(Movie.description).like(search_term),
        ))
    )

    # Subquery for movies matching by genre
    genre_subq = (
        select(Movie.id)
        .join(MovieGenre).join(Genre)
        .filter(func.lower(Genre.name).like(search_term))
    )

    # Subquery for movies matching by director
    director_subq = (
        select(Movie.id)
        .join(MovieDirector).join(Director)
        .filter(func.lower(Director.name).like(search_term))
    )

    # Subquery for movies matching by actor
    actor_subq = (
        select(Movie.id)
        .join(MovieActor).join(Actor)
        .filter(func.lower(Actor.name).like(search_term))
    )

    # Combine all matching IDs with UNION and fetch movies with relationships
    combined_ids = fields_subq.union(genre_subq, director_subq, actor_subq).subquery()

    result = await session.execute(
        select(Movie)
        .options(selectinload(Movie.genre_list), selectinload(Movie.director_list), selectinload(Movie.actor_list))
        .filter(Movie.id.in_(select(combined_ids.c.id)))
    )

    return list(result.unique().scalars().all())


# =============================================================================
# Recommendations Cache
# =============================================================================

async def get_cached_recommendations(session: AsyncSession, tmdb_id: int, is_tv: bool, max_age_days: int = 7) -> Optional[list[int]]:
    """Get cached TMDB recommendations."""
    result = await session.execute(
        select(RecommendationCache)
        .filter(RecommendationCache.source_tmdb_id == tmdb_id, RecommendationCache.source_is_tv == is_tv)
    )
    cache = result.scalar_one_or_none()

    if cache is None:
        return None

    cache_updated = cache.updated_at.replace(tzinfo=timezone.utc) if cache.updated_at.tzinfo is None else cache.updated_at
    if utc_now() - cache_updated > timedelta(days=max_age_days):
        return None

    if cache.recommended_ids:
        return json.loads(cache.recommended_ids)
    return []


async def save_cached_recommendations(session: AsyncSession, tmdb_id: int, is_tv: bool, recommended_ids: list[int]):
    """Save TMDB recommendations to cache."""
    result = await session.execute(
        select(RecommendationCache)
        .filter(RecommendationCache.source_tmdb_id == tmdb_id, RecommendationCache.source_is_tv == is_tv)
    )
    cache = result.scalar_one_or_none()

    if cache is None:
        cache = RecommendationCache(
            source_tmdb_id=tmdb_id,
            source_is_tv=is_tv,
            recommended_ids=json.dumps(recommended_ids),
            updated_at=utc_now()
        )
        session.add(cache)
    else:
        cache.recommended_ids = json.dumps(recommended_ids)
        cache.updated_at = utc_now()

    await session.commit()


# =============================================================================
# Wishlist
# =============================================================================

async def is_in_wishlist(session: AsyncSession, movie_id: int) -> bool:
    """Check if movie is in wishlist."""
    result = await session.execute(select(Wishlist).filter(Wishlist.movie_id == movie_id))
    return result.scalar_one_or_none() is not None


async def add_to_wishlist(session: AsyncSession, movie_id: int) -> Wishlist:
    """Add movie to wishlist."""
    result = await session.execute(select(Wishlist).filter(Wishlist.movie_id == movie_id))
    existing = result.scalar_one_or_none()
    if existing:
        return existing

    wishlist_item = Wishlist(movie_id=movie_id)
    session.add(wishlist_item)
    await session.commit()
    await session.refresh(wishlist_item)
    return wishlist_item


async def remove_from_wishlist(session: AsyncSession, movie_id: int) -> bool:
    """Remove movie from wishlist."""
    result = await session.execute(select(Wishlist).filter(Wishlist.movie_id == movie_id))
    item = result.scalar_one_or_none()
    if item:
        await session.delete(item)
        await session.commit()
        return True
    return False


async def get_wishlist(session: AsyncSession) -> list[Wishlist]:
    """Get all wishlist items ordered by added date (newest first)."""
    result = await session.execute(
        select(Wishlist)
        .options(
            selectinload(Wishlist.movie)
            .selectinload(Movie.genre_list)
        )
        .order_by(Wishlist.added_at.desc())
    )
    return list(result.unique().scalars().all())


async def get_wishlist_movie_ids(session: AsyncSession) -> set[int]:
    """Get set of movie IDs in wishlist (for quick lookup)."""
    result = await session.execute(select(Wishlist.movie_id))
    return {row[0] for row in result.all()}
