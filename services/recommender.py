from typing import Optional
from sqlalchemy.orm import Session

from database.models import Movie
from database import get_all_user_ratings, get_entity_rating, get_cached_recommendations, save_cached_recommendations
from api import TMDBAPI


class RecommenderService:
    """Service for movie recommendations based on TMDB similarity and entity ratings."""

    # Scoring weights
    WEIGHT_TMDB_SIMILARITY = 1.0
    WEIGHT_DIRECTOR = 0.8
    WEIGHT_GENRES = 0.5
    WEIGHT_ACTORS = 0.3
    WEIGHT_AGGREGATORS = 0.2

    # Limits
    MAX_RATED_MOVIES_FOR_SIMILARITY = 30  # Only consider top N rated movies

    def __init__(self, tmdb_api: TMDBAPI):
        self.tmdb_api = tmdb_api
        # In-memory cache for current session (backed by DB)
        self._memory_cache: dict[tuple[int, bool], list[int]] = {}

    def calculate_score(self, movie: Movie, session: Session) -> float:
        """Calculate personal score for a movie based on user preferences."""
        score = 0.0

        # 1. TMDB similarity score (main factor)
        score += self.WEIGHT_TMDB_SIMILARITY * self._tmdb_similarity_score(movie, session)

        # 2. Director rating
        if movie.director:
            dir_rating = get_entity_rating(session, 'director', movie.director)
            if dir_rating and dir_rating.avg_rating is not None:
                score += self.WEIGHT_DIRECTOR * (dir_rating.avg_rating - 5)

        # 3. Genres (average of all genres)
        if movie.genres:
            genre_scores = []
            for genre in movie.genres.split(', '):
                genre = genre.strip()
                if genre:
                    g_rating = get_entity_rating(session, 'genre', genre)
                    if g_rating and g_rating.avg_rating is not None:
                        genre_scores.append(g_rating.avg_rating)
            if genre_scores:
                avg_genre = sum(genre_scores) / len(genre_scores)
                score += self.WEIGHT_GENRES * (avg_genre - 5)

        # 4. Actors (top 5)
        if movie.actors:
            actor_scores = []
            actors = [a.strip() for a in movie.actors.split(', ')[:5] if a.strip()]
            for actor in actors:
                a_rating = get_entity_rating(session, 'actor', actor)
                if a_rating and a_rating.avg_rating is not None:
                    actor_scores.append(a_rating.avg_rating)
            if actor_scores:
                avg_actors = sum(actor_scores) / len(actor_scores)
                score += self.WEIGHT_ACTORS * (avg_actors - 5)

        # 5. Aggregator ratings (tiebreaker)
        aggregator_score = self._calculate_aggregator_score(movie)
        score += self.WEIGHT_AGGREGATORS * (aggregator_score - 5)

        return score

    def _tmdb_similarity_score(self, movie: Movie, session: Session) -> float:
        """Calculate score based on TMDB recommendations from rated movies."""
        user_ratings = get_all_user_ratings(session)
        if not user_ratings:
            return 0.0

        # Filter out neutral ratings (5) - they don't affect recommendations
        liked = [ur for ur in user_ratings if ur.rating >= 6]
        disliked = [ur for ur in user_ratings if ur.rating <= 4]

        # Sort and limit each group
        half_limit = self.MAX_RATED_MOVIES_FOR_SIMILARITY // 2
        top_liked = sorted(liked, key=lambda x: x.rating, reverse=True)[:half_limit]
        top_disliked = sorted(disliked, key=lambda x: x.rating)[:half_limit]  # Lowest first

        selected_ratings = top_liked + top_disliked

        total_score = 0.0

        for ur in selected_ratings:
            rated_movie = ur.movie
            if not rated_movie:
                continue

            # Weight: rating of 5 is neutral, below is negative, above is positive
            weight = ur.rating - 5  # Range: -4 to +5

            # Get recommendations for this rated movie (uses DB cache)
            rec_ids = self._get_cached_recommendations(session, rated_movie.kinopoisk_id, rated_movie.is_tv)

            # Check if our movie is in the recommendations
            for i, rec_id in enumerate(rec_ids):
                if rec_id == movie.kinopoisk_id:
                    # Position weight: 1.0 for first, decreasing by 0.05 per position
                    position_weight = max(0.1, 1.0 - (i * 0.05))
                    total_score += weight * position_weight
                    break

        return total_score

    def _get_cached_recommendations(self, session: Session, tmdb_id: int, is_tv: bool) -> list[int]:
        """Get TMDB recommendations with DB caching."""
        cache_key = (tmdb_id, is_tv)

        # Check memory cache first
        if cache_key in self._memory_cache:
            return self._memory_cache[cache_key]

        # Check DB cache
        cached = get_cached_recommendations(session, tmdb_id, is_tv)
        if cached is not None:
            self._memory_cache[cache_key] = cached
            return cached

        # Fetch from API
        if is_tv:
            recs = self.tmdb_api.get_recommendations_tv(tmdb_id)
        else:
            recs = self.tmdb_api.get_recommendations_movie(tmdb_id)

        # Extract IDs
        rec_ids = [r.get('kinopoisk_id') for r in recs if r.get('kinopoisk_id')]

        # Save to DB cache
        save_cached_recommendations(session, tmdb_id, is_tv, rec_ids)
        self._memory_cache[cache_key] = rec_ids

        return rec_ids

    def _calculate_aggregator_score(self, movie: Movie) -> float:
        """Calculate average score from aggregator ratings (normalized to 1-10)."""
        scores = []

        if movie.tmdb_rating:
            scores.append(movie.tmdb_rating)

        if movie.imdb_rating:
            scores.append(movie.imdb_rating)

        if movie.rotten_tomatoes:
            # Convert 0-100 to 1-10
            scores.append(movie.rotten_tomatoes / 10)

        if movie.metacritic:
            # Convert 0-100 to 1-10
            scores.append(movie.metacritic / 10)

        if scores:
            return sum(scores) / len(scores)
        return 5.0  # Neutral if no ratings

    def clear_cache(self):
        """Clear the recommendations cache."""
        self._memory_cache.clear()

    def has_user_ratings(self, session: Session) -> bool:
        """Check if user has any rated movies."""
        user_ratings = get_all_user_ratings(session)
        return len(user_ratings) > 0
