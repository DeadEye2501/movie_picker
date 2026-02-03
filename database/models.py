from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, Text, LargeBinary, DateTime, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utc_now():
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


# =============================================================================
# M2M Association Tables
# =============================================================================

class MovieTag(Base):
    """Many-to-many relationship between movies and user tags."""
    __tablename__ = "movie_tags"

    movie_id = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True)
    tag_id = Column(Integer, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True)


class MovieGenre(Base):
    """Many-to-many relationship between movies and genres."""
    __tablename__ = "movie_genres"

    movie_id = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True)
    genre_id = Column(Integer, ForeignKey("genres.id", ondelete="CASCADE"), primary_key=True)


class MovieDirector(Base):
    """Many-to-many relationship between movies and directors."""
    __tablename__ = "movie_directors"

    movie_id = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True)
    director_id = Column(Integer, ForeignKey("directors.id", ondelete="CASCADE"), primary_key=True)


class MovieActor(Base):
    """Many-to-many relationship between movies and actors."""
    __tablename__ = "movie_actors"

    movie_id = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True)
    actor_id = Column(Integer, ForeignKey("actors.id", ondelete="CASCADE"), primary_key=True)
    order = Column(Integer, nullable=True)  # Actor's billing order in the movie


# =============================================================================
# Entity Tables (справочники)
# =============================================================================

class Genre(Base):
    """Genres with optional user rating stats."""
    __tablename__ = "genres"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)  # Canonical name: "боевик"
    aliases = Column(String(500), nullable=True)  # Comma-separated: "action, экшен"
    tmdb_movie_id = Column(Integer, nullable=True)  # TMDB movie genre ID
    tmdb_tv_id = Column(Integer, nullable=True)  # TMDB TV genre ID
    # User rating stats (calculated from rated movies)
    avg_rating = Column(Float, nullable=True)
    rating_count = Column(Integer, default=0)

    movies = relationship("Movie", secondary="movie_genres", back_populates="genre_list")

    def __repr__(self):
        return f"<Genre(name='{self.name}', avg={self.avg_rating})>"


class Director(Base):
    """Directors with optional user rating stats."""
    __tablename__ = "directors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tmdb_id = Column(Integer, nullable=True, unique=True, index=True)  # TMDB person ID
    name = Column(String(500), nullable=False)
    # User rating stats (calculated from rated movies)
    avg_rating = Column(Float, nullable=True)
    rating_count = Column(Integer, default=0)

    movies = relationship("Movie", secondary="movie_directors", back_populates="director_list")

    def __repr__(self):
        return f"<Director(name='{self.name}', tmdb_id={self.tmdb_id})>"


class Actor(Base):
    """Actors with optional user rating stats."""
    __tablename__ = "actors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tmdb_id = Column(Integer, nullable=True, unique=True, index=True)  # TMDB person ID
    name = Column(String(500), nullable=False)
    # User rating stats (calculated from rated movies)
    avg_rating = Column(Float, nullable=True)
    rating_count = Column(Integer, default=0)

    movies = relationship("Movie", secondary="movie_actors", back_populates="actor_list")

    def __repr__(self):
        return f"<Actor(name='{self.name}', tmdb_id={self.tmdb_id})>"


class Tag(Base):
    """User-created tags for organizing rated movies."""
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=utc_now)

    movies = relationship("Movie", secondary="movie_tags", back_populates="tag_list")

    def __repr__(self):
        return f"<Tag(name='{self.name}')>"


# =============================================================================
# Main Tables
# =============================================================================

class Movie(Base):
    __tablename__ = "movies"
    __table_args__ = (
        UniqueConstraint('kinopoisk_id', 'is_tv', name='uq_kp_id_type'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    kinopoisk_id = Column(Integer, nullable=False, index=True)  # TMDB ID
    is_tv = Column(Boolean, default=False, nullable=False)  # True for TV shows
    imdb_id = Column(String(20), nullable=True, index=True)
    title = Column(String(500), nullable=False)
    title_original = Column(String(500), nullable=True)
    year = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    poster_url = Column(String(1000), nullable=True)
    # Ratings from different sources
    tmdb_rating = Column(Float, nullable=True)
    kp_rating = Column(Float, nullable=True)  # Kinopoisk rating
    imdb_rating = Column(Float, nullable=True)
    rotten_tomatoes = Column(Integer, nullable=True)  # percentage 0-100
    metacritic = Column(Integer, nullable=True)  # score 0-100
    embedding = Column(LargeBinary, nullable=True)
    created_at = Column(DateTime, default=utc_now)

    # Relationships
    user_rating = relationship("UserRating", back_populates="movie", uselist=False)
    genre_list = relationship("Genre", secondary="movie_genres", back_populates="movies", lazy="selectin")
    director_list = relationship("Director", secondary="movie_directors", back_populates="movies", lazy="selectin")
    actor_list = relationship("Actor", secondary="movie_actors", back_populates="movies", lazy="selectin")
    tag_list = relationship("Tag", secondary="movie_tags", back_populates="movies", lazy="selectin")

    @property
    def genres_display(self) -> str:
        """Get comma-separated genre names."""
        return ", ".join(g.name for g in self.genre_list)

    @property
    def directors_display(self) -> str:
        """Get comma-separated director names."""
        return ", ".join(d.name for d in self.director_list)

    @property
    def actors_display(self) -> str:
        """Get comma-separated actor names."""
        return ", ".join(a.name for a in self.actor_list)

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


class Wishlist(Base):
    """Movies user wants to watch."""
    __tablename__ = "wishlist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    movie_id = Column(Integer, ForeignKey("movies.id"), nullable=False, unique=True)
    added_at = Column(DateTime, default=utc_now)

    movie = relationship("Movie")

    def __repr__(self):
        return f"<Wishlist(movie_id={self.movie_id})>"


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
