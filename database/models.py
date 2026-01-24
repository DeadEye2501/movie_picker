from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, Text, LargeBinary, DateTime, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utc_now():
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class Movie(Base):
    __tablename__ = "movies"
    __table_args__ = (
        UniqueConstraint('kinopoisk_id', 'is_tv', name='uq_tmdb_id_type'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    kinopoisk_id = Column(Integer, nullable=False, index=True)  # TMDB ID
    is_tv = Column(Boolean, default=False, nullable=False)  # True for TV shows
    imdb_id = Column(String(20), nullable=True, index=True)
    title = Column(String(500), nullable=False)
    title_original = Column(String(500), nullable=True)
    year = Column(Integer, nullable=True)
    genres = Column(String(500), nullable=True)
    director = Column(String(500), nullable=True)
    actors = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    poster_url = Column(String(1000), nullable=True)
    # Ratings from different sources
    tmdb_rating = Column(Float, nullable=True)
    imdb_rating = Column(Float, nullable=True)
    rotten_tomatoes = Column(Integer, nullable=True)  # percentage 0-100
    metacritic = Column(Integer, nullable=True)  # score 0-100
    embedding = Column(LargeBinary, nullable=True)
    created_at = Column(DateTime, default=utc_now)

    user_rating = relationship("UserRating", back_populates="movie", uselist=False)

    def __repr__(self):
        return f"<Movie(id={self.id}, title='{self.title}', year={self.year})>"


class UserRating(Base):
    __tablename__ = "user_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    movie_id = Column(Integer, ForeignKey("movies.id"), nullable=False, unique=True)
    rating = Column(Integer, nullable=False)
    review = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    movie = relationship("Movie", back_populates="user_rating")

    def __repr__(self):
        return f"<UserRating(movie_id={self.movie_id}, rating={self.rating})>"


class Genre(Base):
    """Reference table for canonical genre names and aliases."""
    __tablename__ = "genres"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)  # Canonical name: "боевик"
    aliases = Column(String(500), nullable=True)  # Comma-separated: "action, экшен"
    tmdb_movie_id = Column(Integer, nullable=True)  # TMDB movie genre ID
    tmdb_tv_id = Column(Integer, nullable=True)  # TMDB TV genre ID

    def __repr__(self):
        return f"<Genre(name='{self.name}')>"


class GenreRating(Base):
    """Stores aggregated ratings for genres."""
    __tablename__ = "genre_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    genre_id = Column(Integer, ForeignKey("genres.id"), nullable=False, unique=True)
    avg_rating = Column(Float, nullable=True)
    count = Column(Integer, default=0)

    genre = relationship("Genre")

    def __repr__(self):
        return f"<GenreRating(genre_id={self.genre_id}, avg={self.avg_rating}, count={self.count})>"


class DirectorRating(Base):
    """Stores aggregated ratings for directors."""
    __tablename__ = "director_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    director_name = Column(String(500), nullable=False, unique=True)
    avg_rating = Column(Float, nullable=True)
    count = Column(Integer, default=0)

    def __repr__(self):
        return f"<DirectorRating(name='{self.director_name}', avg={self.avg_rating}, count={self.count})>"


class ActorRating(Base):
    """Stores aggregated ratings for actors."""
    __tablename__ = "actor_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    actor_name = Column(String(500), nullable=False, unique=True)
    avg_rating = Column(Float, nullable=True)
    count = Column(Integer, default=0)

    def __repr__(self):
        return f"<ActorRating(name='{self.actor_name}', avg={self.avg_rating}, count={self.count})>"


class RecommendationCache(Base):
    """Caches TMDB recommendations for rated movies."""
    __tablename__ = "recommendation_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_tmdb_id = Column(Integer, nullable=False, index=True)
    source_is_tv = Column(Boolean, default=False, nullable=False)
    recommended_ids = Column(Text, nullable=True)  # JSON list of TMDB IDs
    updated_at = Column(DateTime, default=utc_now)

    __table_args__ = (
        UniqueConstraint('source_tmdb_id', 'source_is_tv', name='uq_source_tmdb'),
    )
