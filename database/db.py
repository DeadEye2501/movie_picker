import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import json
from datetime import timedelta
from .models import Base, Movie, UserRating, EntityRating, RecommendationCache, utc_now

_engine = None
_SessionLocal = None


def init_db(db_path: str = "movie_picker.db"):
    """Initialize the database and create tables."""
    global _engine, _SessionLocal

    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    _engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(_engine)
    _SessionLocal = sessionmaker(bind=_engine)


def get_session() -> Session:
    """Get a new database session."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _SessionLocal(expire_on_commit=False)


def get_movie_by_kp_id(session: Session, kinopoisk_id: int, is_tv: bool = False) -> Optional[Movie]:
    """Get a movie/TV show by its TMDB ID and type."""
    return session.query(Movie).filter(
        Movie.kinopoisk_id == kinopoisk_id,
        Movie.is_tv == is_tv
    ).first()


def save_movie(session: Session, movie_data: dict) -> Movie:
    """Save or update a movie/TV show in the database."""
    is_tv = movie_data.get("is_tv", False)
    movie = get_movie_by_kp_id(session, movie_data["kinopoisk_id"], is_tv)

    if movie is None:
        movie = Movie(**movie_data)
        session.add(movie)
    else:
        for key, value in movie_data.items():
            if hasattr(movie, key) and key != "id":
                setattr(movie, key, value)

    session.commit()
    session.refresh(movie)
    return movie


def get_user_rating(session: Session, movie_id: int) -> Optional[UserRating]:
    """Get user rating for a movie."""
    return session.query(UserRating).filter(UserRating.movie_id == movie_id).first()


def save_user_rating(session: Session, movie_id: int, rating: int, review: Optional[str] = None) -> UserRating:
    """Save or update user rating for a movie."""
    user_rating = get_user_rating(session, movie_id)

    if user_rating is None:
        user_rating = UserRating(movie_id=movie_id, rating=rating, review=review)
        session.add(user_rating)
    else:
        user_rating.rating = rating
        if review is not None:
            user_rating.review = review

    session.commit()
    session.refresh(user_rating)

    # Update entity ratings (genres, director, actors)
    movie = session.query(Movie).get(movie_id)
    if movie:
        update_entity_ratings(session, movie, rating)
        session.commit()

    return user_rating


def get_all_user_ratings(session: Session) -> list[UserRating]:
    """Get all user ratings with their associated movies."""
    return session.query(UserRating).all()


def get_rated_movies(session: Session, min_rating: Optional[int] = None) -> list[Movie]:
    """Get all movies that have user ratings, optionally filtered by minimum rating."""
    query = session.query(Movie).join(UserRating)
    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    return query.all()


def search_local_movies(session: Session, query: str) -> list[Movie]:
    """Search movies in local database by title, director, actors, or description."""
    search_term = f"%{query.lower()}%"
    from sqlalchemy import func
    return session.query(Movie).filter(
        (func.lower(Movie.title).like(search_term)) |
        (func.lower(Movie.title_original).like(search_term)) |
        (func.lower(Movie.director).like(search_term)) |
        (func.lower(Movie.actors).like(search_term)) |
        (func.lower(Movie.genres).like(search_term)) |
        (func.lower(Movie.description).like(search_term))
    ).all()


def get_entity_rating(session: Session, entity_type: str, entity_name: str) -> Optional[EntityRating]:
    """Get entity rating by type and name."""
    return session.query(EntityRating).filter(
        EntityRating.entity_type == entity_type,
        EntityRating.entity_name == entity_name.lower().strip()
    ).first()


def update_entity_ratings(session: Session, movie: Movie, user_rating: int):
    """Update entity ratings based on a movie's user rating."""
    entities_to_update = []

    # Collect genres
    if movie.genres:
        for genre in movie.genres.split(', '):
            genre = genre.strip().lower()
            if genre:
                entities_to_update.append(('genre', genre))

    # Collect director
    if movie.director:
        director = movie.director.strip().lower()
        if director:
            entities_to_update.append(('director', director))

    # Collect actors (top 5)
    if movie.actors:
        actors = [a.strip().lower() for a in movie.actors.split(', ')[:5] if a.strip()]
        for actor in actors:
            entities_to_update.append(('actor', actor))

    # Update each entity
    for entity_type, entity_name in entities_to_update:
        _update_single_entity_rating(session, entity_type, entity_name)


def _update_single_entity_rating(session: Session, entity_type: str, entity_name: str):
    """Recalculate average rating for a single entity based on all user ratings.

    Note: We filter in Python because SQLite's LOWER() doesn't work with Cyrillic.
    """
    # Get all movies with user ratings
    all_rated_movies = session.query(Movie).join(UserRating).all()

    # Filter in Python (case-insensitive for Cyrillic)
    entity_name_lower = entity_name.lower()
    movies_with_entity = []

    for movie in all_rated_movies:
        matches = False
        if entity_type == 'genre' and movie.genres:
            # Check if any genre matches
            movie_genres = [g.strip().lower() for g in movie.genres.split(', ')]
            matches = entity_name_lower in movie_genres
        elif entity_type == 'director' and movie.director:
            matches = movie.director.lower() == entity_name_lower
        elif entity_type == 'actor' and movie.actors:
            # Check if actor is in the list
            movie_actors = [a.strip().lower() for a in movie.actors.split(', ')]
            matches = entity_name_lower in movie_actors

        if matches:
            movies_with_entity.append(movie)

    if not movies_with_entity:
        return

    # Calculate average
    total_rating = sum(m.user_rating.rating for m in movies_with_entity)
    count = len(movies_with_entity)
    avg_rating = total_rating / count

    # Update or create entity rating
    entity_rating = get_entity_rating(session, entity_type, entity_name)
    if entity_rating is None:
        entity_rating = EntityRating(
            entity_type=entity_type,
            entity_name=entity_name,
            avg_rating=avg_rating,
            count=count
        )
        session.add(entity_rating)
    else:
        entity_rating.avg_rating = avg_rating
        entity_rating.count = count


def get_cached_recommendations(session: Session, tmdb_id: int, is_tv: bool, max_age_days: int = 7) -> Optional[list[int]]:
    """Get cached TMDB recommendations. Returns None if not cached or expired."""
    cache = session.query(RecommendationCache).filter(
        RecommendationCache.source_tmdb_id == tmdb_id,
        RecommendationCache.source_is_tv == is_tv
    ).first()

    if cache is None:
        return None

    # Check if cache is expired
    if utc_now() - cache.updated_at > timedelta(days=max_age_days):
        return None

    if cache.recommended_ids:
        return json.loads(cache.recommended_ids)
    return []


def save_cached_recommendations(session: Session, tmdb_id: int, is_tv: bool, recommended_ids: list[int]):
    """Save TMDB recommendations to cache."""
    cache = session.query(RecommendationCache).filter(
        RecommendationCache.source_tmdb_id == tmdb_id,
        RecommendationCache.source_is_tv == is_tv
    ).first()

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

    session.commit()
