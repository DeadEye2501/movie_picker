import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import json
from datetime import timedelta, timezone
from .models import Base, Movie, UserRating, Genre, GenreRating, DirectorRating, ActorRating, RecommendationCache, utc_now
from .genre_utils import GENRE_SEED_DATA, normalize_genres, init_genre_cache, clear_cache

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

    # Seed genres if empty and initialize cache
    session = _SessionLocal()
    try:
        if session.query(Genre).count() == 0:
            _seed_genres(session)
        init_genre_cache(session)
    finally:
        session.close()


def _seed_genres(session: Session):
    """Seed the genres table with canonical genre data."""
    for name, aliases, tmdb_movie_id, tmdb_tv_id in GENRE_SEED_DATA:
        genre = Genre(
            name=name,
            aliases=aliases,
            tmdb_movie_id=tmdb_movie_id,
            tmdb_tv_id=tmdb_tv_id,
        )
        session.add(genre)
    session.commit()


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


def delete_user_rating(session: Session, movie_id: int) -> bool:
    """Delete user rating for a movie and recalculate entity ratings.

    Returns True if rating was deleted, False if it didn't exist.
    """
    user_rating = get_user_rating(session, movie_id)
    if user_rating is None:
        return False

    # Get movie to recalculate entity ratings
    movie = session.query(Movie).get(movie_id)

    # Delete the rating
    session.delete(user_rating)
    session.commit()

    # Recalculate entity ratings
    if movie:
        _recalculate_entity_ratings_for_movie(session, movie)
        session.commit()

    return True


def _recalculate_entity_ratings_for_movie(session: Session, movie: Movie):
    """Recalculate entity ratings for all entities associated with a movie."""
    # Load all rated movies ONCE
    all_rated = session.query(Movie).join(UserRating).all()

    # Pre-compute genre IDs for all rated movies ONCE
    movie_genres_map: dict[int, list[int]] = {}
    for m in all_rated:
        if m.genres:
            movie_genres_map[m.id] = normalize_genres(m.genres, session)

    # Recalculate genre ratings
    if movie.genres:
        genre_ids = normalize_genres(movie.genres, session)
        for genre_id in genre_ids:
            _update_genre_rating(session, genre_id, all_rated, movie_genres_map)

    # Recalculate director rating
    if movie.director:
        _update_director_rating(session, movie.director.strip().lower(), all_rated)

    # Recalculate actor ratings (top 5)
    if movie.actors:
        actors = [a.strip().lower() for a in movie.actors.split(', ')[:5] if a.strip()]
        for actor in actors:
            _update_actor_rating(session, actor, all_rated)


def get_all_user_ratings_filtered(
    session: Session,
    sort_by: str = "date_desc",
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    genres: Optional[list[str]] = None,
) -> list[UserRating]:
    """Get user ratings with sorting and filtering.

    Args:
        session: Database session
        sort_by: Sort option (rating_desc, rating_asc, date_desc, date_asc,
                 year_desc, year_asc, title_asc)
        min_rating: Minimum rating filter (inclusive)
        max_rating: Maximum rating filter (inclusive)
        genres: List of genre names to filter by (case-insensitive, any match)

    Returns:
        List of UserRating objects matching the criteria
    """
    query = session.query(UserRating).join(Movie)

    # Apply rating filter
    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    if max_rating is not None:
        query = query.filter(UserRating.rating <= max_rating)

    # Get all matching ratings first
    user_ratings = query.all()

    # Apply genre filter in Python (SQLite doesn't handle LIKE well with Cyrillic)
    if genres:
        genres_lower = [g.lower() for g in genres]
        filtered = []
        for ur in user_ratings:
            if ur.movie.genres:
                movie_genres = [g.strip().lower() for g in ur.movie.genres.split(', ')]
                if any(g in movie_genres for g in genres_lower):
                    filtered.append(ur)
        user_ratings = filtered

    # Apply sorting
    if sort_by == "rating_desc":
        user_ratings.sort(key=lambda ur: ur.rating, reverse=True)
    elif sort_by == "rating_asc":
        user_ratings.sort(key=lambda ur: ur.rating)
    elif sort_by == "date_desc":
        user_ratings.sort(key=lambda ur: ur.updated_at or ur.created_at, reverse=True)
    elif sort_by == "date_asc":
        user_ratings.sort(key=lambda ur: ur.updated_at or ur.created_at)
    elif sort_by == "year_desc":
        user_ratings.sort(key=lambda ur: ur.movie.year or 0, reverse=True)
    elif sort_by == "year_asc":
        user_ratings.sort(key=lambda ur: ur.movie.year or 0)
    elif sort_by == "title_asc":
        user_ratings.sort(key=lambda ur: ur.movie.title.lower())
    elif sort_by == "title_desc":
        user_ratings.sort(key=lambda ur: ur.movie.title.lower(), reverse=True)

    return user_ratings


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


def get_genre_rating(session: Session, genre_id: int) -> Optional[GenreRating]:
    """Get genre rating by genre ID."""
    return session.query(GenreRating).filter(GenreRating.genre_id == genre_id).first()


def get_director_rating(session: Session, director_name: str) -> Optional[DirectorRating]:
    """Get director rating by name."""
    return session.query(DirectorRating).filter(
        DirectorRating.director_name == director_name.lower().strip()
    ).first()


def get_actor_rating(session: Session, actor_name: str) -> Optional[ActorRating]:
    """Get actor rating by name."""
    return session.query(ActorRating).filter(
        ActorRating.actor_name == actor_name.lower().strip()
    ).first()


def update_entity_ratings(session: Session, movie: Movie, user_rating: int):
    """Update all entity ratings based on a movie's user rating."""
    # Load all rated movies ONCE
    all_rated = session.query(Movie).join(UserRating).all()

    # Pre-compute genre IDs for all rated movies ONCE
    movie_genres_map: dict[int, list[int]] = {}
    for m in all_rated:
        if m.genres:
            movie_genres_map[m.id] = normalize_genres(m.genres, session)

    # Update genre ratings
    if movie.genres:
        genre_ids = normalize_genres(movie.genres, session)
        for genre_id in genre_ids:
            _update_genre_rating(session, genre_id, all_rated, movie_genres_map)

    # Update director rating
    if movie.director:
        _update_director_rating(session, movie.director.strip().lower(), all_rated)

    # Update actor ratings (top 5)
    if movie.actors:
        actors = [a.strip().lower() for a in movie.actors.split(', ')[:5] if a.strip()]
        for actor in actors:
            _update_actor_rating(session, actor, all_rated)


def _update_genre_rating(
    session: Session,
    genre_id: int,
    all_rated: list[Movie],
    movie_genres_map: dict[int, list[int]]
):
    """Recalculate average rating for a genre."""
    movies_with_genre = [
        m for m in all_rated
        if genre_id in movie_genres_map.get(m.id, [])
    ]

    if not movies_with_genre:
        rating = get_genre_rating(session, genre_id)
        if rating:
            session.delete(rating)
        return

    total = sum(m.user_rating.rating for m in movies_with_genre)
    count = len(movies_with_genre)
    avg = total / count

    rating = get_genre_rating(session, genre_id)
    if rating is None:
        rating = GenreRating(genre_id=genre_id, avg_rating=avg, count=count)
        session.add(rating)
    else:
        rating.avg_rating = avg
        rating.count = count


def _update_director_rating(session: Session, director_name: str, all_rated: list[Movie]):
    """Recalculate average rating for a director."""
    movies_with_director = [
        m for m in all_rated
        if m.director and m.director.lower() == director_name
    ]

    if not movies_with_director:
        rating = get_director_rating(session, director_name)
        if rating:
            session.delete(rating)
        return

    total = sum(m.user_rating.rating for m in movies_with_director)
    count = len(movies_with_director)
    avg = total / count

    rating = get_director_rating(session, director_name)
    if rating is None:
        rating = DirectorRating(director_name=director_name, avg_rating=avg, count=count)
        session.add(rating)
    else:
        rating.avg_rating = avg
        rating.count = count


def _update_actor_rating(session: Session, actor_name: str, all_rated: list[Movie]):
    """Recalculate average rating for an actor."""
    movies_with_actor = []
    for m in all_rated:
        if m.actors:
            movie_actors = [a.strip().lower() for a in m.actors.split(', ')]
            if actor_name in movie_actors:
                movies_with_actor.append(m)

    if not movies_with_actor:
        rating = get_actor_rating(session, actor_name)
        if rating:
            session.delete(rating)
        return

    total = sum(m.user_rating.rating for m in movies_with_actor)
    count = len(movies_with_actor)
    avg = total / count

    rating = get_actor_rating(session, actor_name)
    if rating is None:
        rating = ActorRating(actor_name=actor_name, avg_rating=avg, count=count)
        session.add(rating)
    else:
        rating.avg_rating = avg
        rating.count = count


def get_cached_recommendations(session: Session, tmdb_id: int, is_tv: bool, max_age_days: int = 7) -> Optional[list[int]]:
    """Get cached TMDB recommendations. Returns None if not cached or expired."""
    cache = session.query(RecommendationCache).filter(
        RecommendationCache.source_tmdb_id == tmdb_id,
        RecommendationCache.source_is_tv == is_tv
    ).first()

    if cache is None:
        return None

    # Check if cache is expired
    # SQLite stores datetimes without timezone, so we need to make it aware
    cache_updated = cache.updated_at.replace(tzinfo=timezone.utc) if cache.updated_at.tzinfo is None else cache.updated_at
    if utc_now() - cache_updated > timedelta(days=max_age_days):
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
