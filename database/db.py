import os
from contextlib import contextmanager
from typing import Optional, Generator
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker, Session

import json
from datetime import timedelta, timezone
from .models import (
    Base, Movie, UserRating, Genre, Director, Actor,
    MovieGenre, MovieDirector, MovieActor,
    Wishlist, RecommendationCache, utc_now
)
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


def close_db():
    """Close the database engine and all connections."""
    global _engine, _SessionLocal
    if _engine:
        _engine.dispose()
        _engine = None
        _SessionLocal = None


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Get a database session as context manager."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    session = _SessionLocal(expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()


# =============================================================================
# Movie CRUD
# =============================================================================

def get_movie_by_kp_id(session: Session, kinopoisk_id: int, is_tv: bool = False) -> Optional[Movie]:
    """Get a movie/TV show by its TMDB ID and type."""
    return session.query(Movie).filter(
        Movie.kinopoisk_id == kinopoisk_id,
        Movie.is_tv == is_tv
    ).first()


def save_movie(session: Session, movie_data: dict) -> Movie:
    """Save or update a movie/TV show in the database.

    Handles M2M relationships for genres, directors, and actors.
    Expected keys in movie_data:
        - genres: comma-separated string "драма, комедия"
        - directors: list of dicts [{"tmdb_id": 123, "name": "Name"}, ...]
        - actors: list of dicts [{"tmdb_id": 456, "name": "Name"}, ...]
    """
    is_tv = movie_data.get("is_tv", False)
    kinopoisk_id = movie_data["kinopoisk_id"]
    movie = get_movie_by_kp_id(session, kinopoisk_id, is_tv)

    # Extract M2M data
    genres_string = movie_data.pop("genres", None)
    directors_list = movie_data.pop("directors", None)
    actors_list = movie_data.pop("actors", None)

    try:
        if movie is None:
            movie = Movie(**movie_data)
            session.add(movie)
            session.flush()  # Get movie.id before setting M2M
        else:
            for key, value in movie_data.items():
                if hasattr(movie, key) and key != "id":
                    setattr(movie, key, value)

        # Set M2M relationships
        if genres_string is not None:
            set_movie_genres(session, movie, genres_string)
        if directors_list is not None:
            set_movie_directors(session, movie, directors_list)
        if actors_list is not None:
            set_movie_actors(session, movie, actors_list)

        session.commit()
        session.refresh(movie)
    except Exception:
        session.rollback()
        # Re-fetch movie after rollback if it existed
        movie = get_movie_by_kp_id(session, kinopoisk_id, is_tv)
        if movie is None:
            raise

    return movie


# =============================================================================
# M2M Setters
# =============================================================================

def set_movie_genres(session: Session, movie: Movie, genres_string: str):
    """Set movie genres from a comma-separated string."""
    session.query(MovieGenre).filter(MovieGenre.movie_id == movie.id).delete()

    if not genres_string:
        return

    genre_ids = normalize_genres(genres_string, session)
    seen_genre_ids = set()
    for genre_id in genre_ids:
        if genre_id not in seen_genre_ids:
            seen_genre_ids.add(genre_id)
            session.add(MovieGenre(movie_id=movie.id, genre_id=genre_id))


def set_movie_directors(session: Session, movie: Movie, directors: list[dict]):
    """Set movie directors from a list of dicts with tmdb_id and name."""
    session.query(MovieDirector).filter(MovieDirector.movie_id == movie.id).delete()

    if not directors:
        return

    seen_tmdb_ids = set()

    for d in directors:
        tmdb_id = d.get("tmdb_id")
        name = d.get("name")
        if not tmdb_id or not name:
            continue
        if tmdb_id in seen_tmdb_ids:
            continue
        seen_tmdb_ids.add(tmdb_id)

        director = get_or_create_director(session, tmdb_id, name)
        session.add(MovieDirector(movie_id=movie.id, director_id=director.id))


def set_movie_actors(session: Session, movie: Movie, actors: list[dict], limit: int = 10):
    """Set movie actors from a list of dicts with tmdb_id and name."""
    session.query(MovieActor).filter(MovieActor.movie_id == movie.id).delete()

    if not actors:
        return

    seen_tmdb_ids = set()
    order = 0

    for a in actors[:limit]:
        tmdb_id = a.get("tmdb_id")
        name = a.get("name")
        if not tmdb_id or not name:
            continue
        if tmdb_id in seen_tmdb_ids:
            continue
        seen_tmdb_ids.add(tmdb_id)

        actor = get_or_create_actor(session, tmdb_id, name)
        session.add(MovieActor(movie_id=movie.id, actor_id=actor.id, order=order))
        order += 1


# =============================================================================
# Entity Getters/Creators
# =============================================================================

def get_or_create_director(session: Session, tmdb_id: int, name: str) -> Director:
    """Get existing director by TMDB ID or create new one."""
    director = session.query(Director).filter(Director.tmdb_id == tmdb_id).first()
    if director is None:
        director = Director(tmdb_id=tmdb_id, name=name)
        session.add(director)
        session.flush()
    elif director.name != name:
        # Update name if changed (e.g., different localization)
        director.name = name
    return director


def get_or_create_actor(session: Session, tmdb_id: int, name: str) -> Actor:
    """Get existing actor by TMDB ID or create new one."""
    actor = session.query(Actor).filter(Actor.tmdb_id == tmdb_id).first()
    if actor is None:
        actor = Actor(tmdb_id=tmdb_id, name=name)
        session.add(actor)
        session.flush()
    elif actor.name != name:
        # Update name if changed (e.g., different localization)
        actor.name = name
    return actor


def get_genre_by_id(session: Session, genre_id: int) -> Optional[Genre]:
    """Get genre by ID."""
    return session.query(Genre).filter(Genre.id == genre_id).first()


def get_director_by_id(session: Session, director_id: int) -> Optional[Director]:
    """Get director by ID."""
    return session.query(Director).filter(Director.id == director_id).first()


def get_actor_by_id(session: Session, actor_id: int) -> Optional[Actor]:
    """Get actor by ID."""
    return session.query(Actor).filter(Actor.id == actor_id).first()


# =============================================================================
# User Ratings
# =============================================================================

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

    # Remove from wishlist if present (watched = no longer "want to watch")
    wishlist_item = session.query(Wishlist).filter(Wishlist.movie_id == movie_id).first()
    if wishlist_item:
        session.delete(wishlist_item)

    session.commit()
    session.refresh(user_rating)

    # Update entity ratings (genres, directors, actors)
    movie = session.get(Movie, movie_id)
    if movie:
        update_entity_ratings(session, movie)
        session.commit()

    return user_rating


def delete_user_rating(session: Session, movie_id: int) -> bool:
    """Delete user rating for a movie and recalculate entity ratings."""
    user_rating = get_user_rating(session, movie_id)
    if user_rating is None:
        return False

    movie = session.get(Movie, movie_id)
    session.delete(user_rating)
    session.commit()

    if movie:
        update_entity_ratings(session, movie)
        session.commit()

    return True


def get_all_user_ratings(session: Session) -> list[UserRating]:
    """Get all user ratings with their associated movies."""
    return session.query(UserRating).all()


def get_all_user_ratings_filtered(
    session: Session,
    sort_by: str = "date_desc",
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    genres: Optional[list[str]] = None,
) -> list[UserRating]:
    """Get user ratings with sorting and filtering."""
    query = session.query(UserRating).join(Movie)

    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    if max_rating is not None:
        query = query.filter(UserRating.rating <= max_rating)

    user_ratings = query.all()

    # Apply genre filter using M2M relationship
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


def get_rated_movies(session: Session, min_rating: Optional[int] = None) -> list[Movie]:
    """Get all movies that have user ratings."""
    query = session.query(Movie).join(UserRating)
    if min_rating is not None:
        query = query.filter(UserRating.rating >= min_rating)
    return query.all()


# =============================================================================
# Entity Rating Calculations
# =============================================================================

def update_entity_ratings(session: Session, movie: Movie):
    """Update ratings for all entities (genres, directors, actors) related to a movie."""
    # Update genres
    for genre in movie.genre_list:
        _recalculate_genre_rating(session, genre)

    # Update directors
    for director in movie.director_list:
        _recalculate_director_rating(session, director)

    # Update actors
    for actor in movie.actor_list:
        _recalculate_actor_rating(session, actor)


def _recalculate_genre_rating(session: Session, genre: Genre):
    """Recalculate average rating for a genre."""
    # Get all rated movies with this genre
    rated_movies = session.query(Movie).join(UserRating).join(MovieGenre).filter(
        MovieGenre.genre_id == genre.id
    ).all()

    if not rated_movies:
        genre.avg_rating = None
        genre.rating_count = 0
    else:
        total = sum(m.user_rating.rating for m in rated_movies)
        genre.avg_rating = total / len(rated_movies)
        genre.rating_count = len(rated_movies)


def _recalculate_director_rating(session: Session, director: Director):
    """Recalculate average rating for a director."""
    rated_movies = session.query(Movie).join(UserRating).join(MovieDirector).filter(
        MovieDirector.director_id == director.id
    ).all()

    if not rated_movies:
        director.avg_rating = None
        director.rating_count = 0
    else:
        total = sum(m.user_rating.rating for m in rated_movies)
        director.avg_rating = total / len(rated_movies)
        director.rating_count = len(rated_movies)


def _recalculate_actor_rating(session: Session, actor: Actor):
    """Recalculate average rating for an actor."""
    rated_movies = session.query(Movie).join(UserRating).join(MovieActor).filter(
        MovieActor.actor_id == actor.id
    ).all()

    if not rated_movies:
        actor.avg_rating = None
        actor.rating_count = 0
    else:
        total = sum(m.user_rating.rating for m in rated_movies)
        actor.avg_rating = total / len(rated_movies)
        actor.rating_count = len(rated_movies)


# =============================================================================
# Search
# =============================================================================

def search_local_movies(session: Session, query: str) -> list[Movie]:
    """Search movies in local database by title, description, genres, directors, or actors."""
    search_term = f"%{query.lower()}%"

    # Search in movie fields
    movies_by_fields = session.query(Movie).filter(
        or_(
            func.lower(Movie.title).like(search_term),
            func.lower(Movie.title_original).like(search_term),
            func.lower(Movie.description).like(search_term),
        )
    ).all()

    # Search by genre name
    movies_by_genre = session.query(Movie).join(MovieGenre).join(Genre).filter(
        func.lower(Genre.name).like(search_term)
    ).all()

    # Search by director name
    movies_by_director = session.query(Movie).join(MovieDirector).join(Director).filter(
        func.lower(Director.name).like(search_term)
    ).all()

    # Search by actor name
    movies_by_actor = session.query(Movie).join(MovieActor).join(Actor).filter(
        func.lower(Actor.name).like(search_term)
    ).all()

    # Combine results (unique movies)
    movie_ids = set()
    result = []
    for movie in movies_by_fields + movies_by_genre + movies_by_director + movies_by_actor:
        if movie.id not in movie_ids:
            movie_ids.add(movie.id)
            result.append(movie)

    return result


# =============================================================================
# Recommendations Cache
# =============================================================================

def get_cached_recommendations(session: Session, tmdb_id: int, is_tv: bool, max_age_days: int = 7) -> Optional[list[int]]:
    """Get cached TMDB recommendations. Returns None if not cached or expired."""
    cache = session.query(RecommendationCache).filter(
        RecommendationCache.source_tmdb_id == tmdb_id,
        RecommendationCache.source_is_tv == is_tv
    ).first()

    if cache is None:
        return None

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


# =============================================================================
# Wishlist
# =============================================================================

def is_in_wishlist(session: Session, movie_id: int) -> bool:
    """Check if movie is in wishlist."""
    return session.query(Wishlist).filter(Wishlist.movie_id == movie_id).first() is not None


def add_to_wishlist(session: Session, movie_id: int) -> Wishlist:
    """Add movie to wishlist."""
    existing = session.query(Wishlist).filter(Wishlist.movie_id == movie_id).first()
    if existing:
        return existing

    wishlist_item = Wishlist(movie_id=movie_id)
    session.add(wishlist_item)
    session.commit()
    session.refresh(wishlist_item)
    return wishlist_item


def remove_from_wishlist(session: Session, movie_id: int) -> bool:
    """Remove movie from wishlist."""
    item = session.query(Wishlist).filter(Wishlist.movie_id == movie_id).first()
    if item:
        session.delete(item)
        session.commit()
        return True
    return False


def get_wishlist(session: Session) -> list[Wishlist]:
    """Get all wishlist items ordered by added date (newest first)."""
    return session.query(Wishlist).order_by(Wishlist.added_at.desc()).all()


def get_wishlist_movie_ids(session: Session) -> set[int]:
    """Get set of movie IDs in wishlist (for quick lookup)."""
    items = session.query(Wishlist.movie_id).all()
    return {item[0] for item in items}
