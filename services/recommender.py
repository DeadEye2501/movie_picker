from typing import Optional
from collections import OrderedDict
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Movie
from database import (
    get_all_user_ratings,
    get_cached_recommendations, get_cached_recommendations_batch, save_cached_recommendations
)
from api import TMDBAPI


class LRUCache:
    """Simple LRU cache with max size."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._cache: OrderedDict = OrderedDict()

    def get(self, key):
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def set(self, key, value):
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
        self._cache[key] = value

    def __contains__(self, key):
        return key in self._cache

    def clear(self):
        self._cache.clear()


class RecommenderService:
    """Service for movie recommendations based on TMDB similarity and entity ratings."""

    # Scoring weights
    WEIGHT_TMDB_SIMILARITY = 1.0
    WEIGHT_DIRECTOR = 0.8
    WEIGHT_GENRES = 0.5
    WEIGHT_ACTORS = 0.3
    WEIGHT_AGGREGATORS = 0.2

    # Limits
    MAX_RATED_MOVIES_FOR_SIMILARITY = 30
    MAX_CACHE_SIZE = 200  # Limit in-memory cache

    def __init__(self, tmdb_api: TMDBAPI):
        self.tmdb_api = tmdb_api
        # In-memory LRU cache for current session (backed by DB)
        self._memory_cache = LRUCache(max_size=self.MAX_CACHE_SIZE)

    async def calculate_score(
        self,
        movie: Movie,
        session: AsyncSession,
        cached_ratings: list = None,
        preloaded_recommendations: dict = None
    ) -> float:
        """Calculate personal score for a movie based on user preferences.

        Args:
            movie: Movie to score
            session: Database session
            cached_ratings: Pre-loaded user ratings to avoid N+1 queries.
                           If None, will fetch from DB (slower for batch operations).
            preloaded_recommendations: Pre-loaded recommendations dict mapping
                           (tmdb_id, is_tv) -> list[int]. If provided, skips DB queries.
        """
        score = 0.0

        # 1. TMDB similarity score (main factor)
        score += self.WEIGHT_TMDB_SIMILARITY * await self._tmdb_similarity_score(
            movie, session, cached_ratings, preloaded_recommendations
        )

        # 2. Director rating (from M2M relationship)
        if movie.director_list:
            dir_scores = []
            for director in movie.director_list:
                if director.avg_rating is not None:
                    dir_scores.append(director.avg_rating)
            if dir_scores:
                avg_dir = sum(dir_scores) / len(dir_scores)
                score += self.WEIGHT_DIRECTOR * (avg_dir - 5)

        # 3. Genres (from M2M relationship)
        if movie.genre_list:
            genre_scores = []
            for genre in movie.genre_list:
                if genre.avg_rating is not None:
                    genre_scores.append(genre.avg_rating)
            if genre_scores:
                avg_genre = sum(genre_scores) / len(genre_scores)
                score += self.WEIGHT_GENRES * (avg_genre - 5)

        # 4. Actors (from M2M relationship)
        if movie.actor_list:
            actor_scores = []
            for actor in movie.actor_list[:5]:  # Top 5 actors
                if actor.avg_rating is not None:
                    actor_scores.append(actor.avg_rating)
            if actor_scores:
                avg_actors = sum(actor_scores) / len(actor_scores)
                score += self.WEIGHT_ACTORS * (avg_actors - 5)

        # 5. Aggregator ratings (tiebreaker)
        aggregator_score = self._calculate_aggregator_score(movie)
        score += self.WEIGHT_AGGREGATORS * (aggregator_score - 5)

        return score

    async def _tmdb_similarity_score(
        self,
        movie: Movie,
        session: AsyncSession,
        cached_ratings: list = None,
        preloaded_recommendations: dict = None
    ) -> float:
        """Calculate score based on TMDB recommendations from rated movies."""
        # Use cached ratings if provided, otherwise fetch (slower)
        if cached_ratings is None:
            user_ratings = await get_all_user_ratings(session)
        else:
            user_ratings = cached_ratings

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

            # Get recommendations - use preloaded if available
            key = (rated_movie.kinopoisk_id, rated_movie.is_tv)
            if preloaded_recommendations is not None and key in preloaded_recommendations:
                rec_ids = preloaded_recommendations[key]
            else:
                rec_ids = await self._get_cached_recommendations(session, rated_movie.kinopoisk_id, rated_movie.is_tv)

            # Check if our movie is in the recommendations
            for i, rec_id in enumerate(rec_ids):
                if rec_id == movie.kinopoisk_id:
                    # Position weight: 1.0 for first, decreasing by 0.05 per position
                    position_weight = max(0.1, 1.0 - (i * 0.05))
                    total_score += weight * position_weight
                    break

        return total_score

    async def _get_cached_recommendations(self, session: AsyncSession, tmdb_id: int, is_tv: bool) -> list[int]:
        """Get TMDB recommendations with DB caching."""
        cache_key = (tmdb_id, is_tv)

        # Check memory cache first (LRU)
        cached_mem = self._memory_cache.get(cache_key)
        if cached_mem is not None:
            return cached_mem

        # Check DB cache
        cached = await get_cached_recommendations(session, tmdb_id, is_tv)
        if cached is not None:
            self._memory_cache.set(cache_key, cached)
            return cached

        # Fetch from API
        if is_tv:
            recs = await self.tmdb_api.get_recommendations_tv(tmdb_id)
        else:
            recs = await self.tmdb_api.get_recommendations_movie(tmdb_id)

        # Extract IDs
        rec_ids = [r.get('kinopoisk_id') for r in recs if r.get('kinopoisk_id')]

        # Save to DB cache
        await save_cached_recommendations(session, tmdb_id, is_tv, rec_ids)
        self._memory_cache.set(cache_key, rec_ids)

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

    async def has_user_ratings(self, session: AsyncSession, cached_ratings: list = None) -> bool:
        """Check if user has any rated movies."""
        if cached_ratings is not None:
            return len(cached_ratings) > 0
        user_ratings = await get_all_user_ratings(session)
        return len(user_ratings) > 0

    async def preload_recommendations(self, session: AsyncSession, cached_ratings: list) -> dict[tuple[int, bool], list[int]]:
        """Preload all recommendations needed for scoring in a single batch query.

        Returns dict mapping (tmdb_id, is_tv) -> list of recommended IDs.
        """
        if not cached_ratings:
            return {}

        # Filter out neutral ratings (5) - they don't affect recommendations
        liked = [ur for ur in cached_ratings if ur.rating >= 6]
        disliked = [ur for ur in cached_ratings if ur.rating <= 4]

        # Sort and limit each group
        half_limit = self.MAX_RATED_MOVIES_FOR_SIMILARITY // 2
        top_liked = sorted(liked, key=lambda x: x.rating, reverse=True)[:half_limit]
        top_disliked = sorted(disliked, key=lambda x: x.rating)[:half_limit]

        selected_ratings = top_liked + top_disliked

        # Collect keys for batch query
        keys = []
        for ur in selected_ratings:
            if ur.movie:
                keys.append((ur.movie.kinopoisk_id, ur.movie.is_tv))

        if not keys:
            return {}

        # Batch fetch from DB
        cached_recs = await get_cached_recommendations_batch(session, keys)

        # Check memory cache and identify missing keys
        result = {}
        missing_keys = []

        for key in keys:
            # Check memory cache first
            cached_mem = self._memory_cache.get(key)
            if cached_mem is not None:
                result[key] = cached_mem
            elif key in cached_recs:
                result[key] = cached_recs[key]
                self._memory_cache.set(key, cached_recs[key])
            else:
                missing_keys.append(key)

        # Fetch missing from API in parallel
        if missing_keys:
            import asyncio
            tasks = []
            for tmdb_id, is_tv in missing_keys:
                if is_tv:
                    tasks.append(self.tmdb_api.get_recommendations_tv(tmdb_id))
                else:
                    tasks.append(self.tmdb_api.get_recommendations_movie(tmdb_id))

            api_results = await asyncio.gather(*tasks, return_exceptions=True)

            for key, api_result in zip(missing_keys, api_results):
                if isinstance(api_result, Exception):
                    result[key] = []
                    continue

                rec_ids = [r.get('kinopoisk_id') for r in api_result if r.get('kinopoisk_id')]
                result[key] = rec_ids
                self._memory_cache.set(key, rec_ids)
                # Save to DB cache
                await save_cached_recommendations(session, key[0], key[1], rec_ids)

        return result
