from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session

from api import TMDBAPI, OMDBAPI
from database import get_movie_by_kp_id, save_movie, search_local_movies
from database.models import Movie
from .recommender import RecommenderService


class SearchService:
    """Service for searching and retrieving movies."""

    def __init__(self, tmdb_api: TMDBAPI, omdb_api: OMDBAPI, recommender: RecommenderService):
        self.tmdb_api = tmdb_api
        self.omdb_api = omdb_api
        self.recommender = recommender

    def search_movies(self, session: Session, query: str, page: int = 1, genres: list[int] = None) -> list[Movie]:
        """
        Search for movies AND TV shows by keyword and/or genres, cache results, and sort by user preferences.
        Uses parallel API requests for speed.
        """
        query_words = [w.lower() for w in query.split() if w.strip()]
        genres = genres or []

        if not query_words and not genres:
            return []

        seen_movie_ids = set()
        seen_tv_ids = set()
        all_movies = []
        all_search_results = []

        from database import get_rated_movies, get_cached_recommendations, save_cached_recommendations

        # 1. Get recommendations from user's rated movies (from cache, no API calls)
        rated_movies = get_rated_movies(session, min_rating=6)[:10]
        uncached_rated = []

        for rated in rated_movies:
            cached_ids = get_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv)
            if cached_ids is not None:
                for rec_id in cached_ids[:10]:
                    target_set = seen_tv_ids if rated.is_tv else seen_movie_ids
                    if rec_id and rec_id not in target_set:
                        target_set.add(rec_id)
                        all_search_results.append({"kinopoisk_id": rec_id, "is_tv": rated.is_tv})
            else:
                uncached_rated.append(rated)

        # Fetch uncached recommendations in parallel
        if uncached_rated:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(
                        self.tmdb_api.get_recommendations_tv if r.is_tv else self.tmdb_api.get_recommendations_movie,
                        r.kinopoisk_id
                    ): r for r in uncached_rated
                }
                for future in as_completed(futures):
                    rated = futures[future]
                    try:
                        recs = future.result()
                        rec_ids = [r.get("kinopoisk_id") for r in recs if r.get("kinopoisk_id")]
                        save_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv, rec_ids)
                        for rec in recs[:10]:
                            rec_id = rec.get("kinopoisk_id")
                            is_tv = rec.get("is_tv", False)
                            target_set = seen_tv_ids if is_tv else seen_movie_ids
                            if rec_id and rec_id not in target_set:
                                target_set.add(rec_id)
                                all_search_results.append(rec)
                    except Exception:
                        pass

        # 2. Parallel API calls for discover and search (3 pages each = ~60 results)
        api_tasks = []

        if genres:
            tv_genres = self._map_movie_genres_to_tv(genres)
            for pg in range(1, 4):
                api_tasks.append(("discover_movie", genres, pg))
                if tv_genres:
                    api_tasks.append(("discover_tv", tv_genres, pg))

        if query_words:
            for word in query_words:
                for pg in range(1, 4):
                    api_tasks.append(("search", word, pg))

        if api_tasks:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {}
                for task in api_tasks:
                    if task[0] == "discover_movie":
                        futures[executor.submit(self.tmdb_api.discover_by_genre, task[1], task[2])] = task
                    elif task[0] == "discover_tv":
                        futures[executor.submit(self.tmdb_api.discover_tv_by_genre, task[1], task[2])] = task
                    elif task[0] == "search":
                        futures[executor.submit(self.tmdb_api.search_by_keyword, task[1], task[2])] = task

                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        results = future.result()
                        for result in results:
                            tmdb_id = result.get("kinopoisk_id")
                            is_tv = result.get("is_tv", False)
                            target_set = seen_tv_ids if is_tv else seen_movie_ids
                            if tmdb_id and tmdb_id not in target_set:
                                target_set.add(tmdb_id)
                                all_search_results.append(result)
                    except Exception:
                        pass

        # 3. Search local database
        if query_words:
            for word in query_words:
                for movie in search_local_movies(session, word):
                    target_set = seen_tv_ids if movie.is_tv else seen_movie_ids
                    if movie.kinopoisk_id not in target_set:
                        target_set.add(movie.kinopoisk_id)
                        all_movies.append(movie)

        # 5. Load full info for API results
        if all_search_results:
            api_movies = self._load_movies_parallel(session, all_search_results)
            all_movies.extend(api_movies)

        # 6. Filter by ALL query words (if any)
        if query_words:
            filtered_movies = []
            for movie in all_movies:
                if self._matches_all_words(movie, query_words):
                    filtered_movies.append(movie)
        else:
            filtered_movies = all_movies

        # 7. Filter by selected genres (if any)
        if genres:
            filtered_movies = [m for m in filtered_movies if self._matches_genres(m, genres)]

        sorted_movies = self._sort_by_user_preference(session, filtered_movies)
        return sorted_movies

    def _map_movie_genres_to_tv(self, movie_genre_ids: list[int]) -> list[int]:
        """Map movie genre IDs to TV genre IDs."""
        # Mapping from movie genre ID to TV genre ID
        mapping = {
            28: 10759,   # Action -> Action & Adventure
            12: 10759,   # Adventure -> Action & Adventure
            16: 16,      # Animation
            35: 35,      # Comedy
            80: 80,      # Crime
            99: 99,      # Documentary
            18: 18,      # Drama
            10751: 10751, # Family
            14: 10765,   # Fantasy -> Sci-Fi & Fantasy
            27: None,    # Horror (no direct TV equivalent)
            10402: None, # Music
            9648: 9648,  # Mystery
            10749: None, # Romance
            878: 10765,  # Sci-Fi -> Sci-Fi & Fantasy
            53: None,    # Thriller
            10752: 10768, # War -> War & Politics
            37: 37,      # Western
        }
        tv_genres = []
        seen = set()
        for gid in movie_genre_ids:
            tv_gid = mapping.get(gid)
            if tv_gid and tv_gid not in seen:
                seen.add(tv_gid)
                tv_genres.append(tv_gid)
        return tv_genres

    def _load_movies_parallel(self, session: Session, search_results: list[dict]) -> list[Movie]:
        """Load movie/TV details in parallel for speed."""
        movies = []
        to_load = []

        for result in search_results:
            kp_id = result.get("kinopoisk_id")
            is_tv = result.get("is_tv", False)
            if not kp_id:
                continue

            existing_movie = get_movie_by_kp_id(session, kp_id, is_tv)
            if existing_movie and existing_movie.director:
                movies.append(existing_movie)
            else:
                to_load.append((kp_id, is_tv))

        if to_load:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self._load_single_item, kp_id, is_tv): (kp_id, is_tv) for kp_id, is_tv in to_load}

                for future in as_completed(futures):
                    try:
                        full_info = future.result()
                        if full_info:
                            movie = save_movie(session, full_info)
                            movies.append(movie)
                    except Exception:
                        pass

            session.commit()

        return movies

    def _load_single_item(self, item_id: int, is_tv: bool) -> Optional[dict]:
        """Load a single movie or TV show with all ratings."""
        if is_tv:
            full_info = self.tmdb_api.get_full_tv_info(item_id)
        else:
            full_info = self.tmdb_api.get_full_movie_info(item_id)

        if not full_info:
            return None

        # Fetch additional ratings from OMDB if available (works for both movies and TV)
        if self.omdb_api and full_info.get("imdb_id"):
            omdb_ratings = self.omdb_api.get_ratings_by_imdb_id(full_info["imdb_id"])
            full_info["imdb_rating"] = omdb_ratings.get("imdb")
            full_info["rotten_tomatoes"] = omdb_ratings.get("rotten_tomatoes")
            full_info["metacritic"] = omdb_ratings.get("metacritic")

        return full_info

    def _matches_all_words(self, movie: Movie, query_words: list[str]) -> bool:
        """Check if movie matches ALL query words (AND logic) using exact substring match."""
        searchable_text = self._get_searchable_text(movie)

        for word in query_words:
            if word not in searchable_text:
                return False

        return True

    def _matches_genres(self, movie: Movie, genre_ids: list[int]) -> bool:
        """Check if movie matches ALL selected genres (AND logic)."""
        if not movie.genres:
            return False

        movie_genres_lower = movie.genres.lower()

        # Map genre IDs to Russian names for matching
        genre_names = {
            28: "боевик", 12: "приключения", 16: "мультфильм", 35: "комедия",
            80: "криминал", 99: "документальный", 18: "драма", 10751: "семейный",
            14: "фэнтези", 36: "история", 27: "ужасы", 10402: "музыка",
            9648: "детектив", 10749: "мелодрама", 878: "фантастика",
            10770: "тв фильм", 53: "триллер", 10752: "военный", 37: "вестерн",
            # TV-specific
            10759: "боевик", 10765: "фантастика", 10762: "детский",
            10763: "новости", 10764: "реалити", 10766: "мыльная опера",
            10767: "ток-шоу", 10768: "военный",
        }

        # AND logic: movie must have ALL selected genres
        for genre_id in genre_ids:
            genre_name = genre_names.get(genre_id, "")
            if genre_name and genre_name not in movie_genres_lower:
                return False  # Missing this genre

        return True

    def _get_searchable_text(self, movie: Movie) -> str:
        """Get all searchable text from movie."""
        parts = []
        if movie.title:
            parts.append(movie.title.lower())
        if movie.title_original:
            parts.append(movie.title_original.lower())
        if movie.genres:
            parts.append(movie.genres.lower())
        if movie.director:
            parts.append(movie.director.lower())
        if movie.actors:
            parts.append(movie.actors.lower())
        if movie.description:
            parts.append(movie.description.lower())

        return " ".join(parts)

    def _sort_by_user_preference(self, session: Session, movies: list[Movie]) -> list[Movie]:
        """
        Sort movies by personal recommendation score.
        Uses TMDB similarity + entity ratings (genres, directors, actors).
        """
        if not movies:
            return movies

        # Check if user has any ratings
        if not self.recommender.has_user_ratings(session):
            # No ratings yet - sort by aggregator ratings
            return sorted(movies, key=lambda m: m.tmdb_rating or 0, reverse=True)

        # Calculate score for each movie
        scored_movies = []
        for movie in movies:
            score = self.recommender.calculate_score(movie, session)
            scored_movies.append((movie, score))

        # Sort by score (descending)
        scored_movies.sort(key=lambda x: x[1], reverse=True)

        return [movie for movie, _ in scored_movies]

    def find_magic_recommendation(self, session: Session) -> Optional[Movie]:
        """
        Find the single best unwatched movie based on user's preferences.
        Collects recommendations from all liked movies, scores them, returns the best.
        """
        from database import get_rated_movies, get_all_user_ratings, get_cached_recommendations, save_cached_recommendations

        # Need at least some ratings
        rated_movies = get_rated_movies(session, min_rating=6)
        if not rated_movies:
            return None

        # Get IDs of already rated movies (to exclude)
        all_ratings = get_all_user_ratings(session)
        rated_ids = {(ur.movie.kinopoisk_id, ur.movie.is_tv) for ur in all_ratings}

        # Collect recommendations from liked movies
        seen_ids = set()
        candidates = []

        # Fetch recommendations in parallel
        uncached = []
        for rated in rated_movies[:20]:
            cached_ids = get_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv)
            if cached_ids is not None:
                for rec_id in cached_ids:
                    key = (rec_id, rated.is_tv)
                    if key not in rated_ids and key not in seen_ids:
                        seen_ids.add(key)
                        candidates.append({"kinopoisk_id": rec_id, "is_tv": rated.is_tv})
            else:
                uncached.append(rated)

        # Fetch uncached in parallel
        if uncached:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(
                        self.tmdb_api.get_recommendations_tv if r.is_tv else self.tmdb_api.get_recommendations_movie,
                        r.kinopoisk_id
                    ): r for r in uncached
                }
                for future in as_completed(futures):
                    rated = futures[future]
                    try:
                        recs = future.result()
                        rec_ids = [r.get("kinopoisk_id") for r in recs if r.get("kinopoisk_id")]
                        save_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv, rec_ids)
                        for rec in recs:
                            rec_id = rec.get("kinopoisk_id")
                            is_tv = rec.get("is_tv", False)
                            key = (rec_id, is_tv)
                            if key not in rated_ids and key not in seen_ids:
                                seen_ids.add(key)
                                candidates.append(rec)
                    except Exception:
                        pass

        if not candidates:
            return None

        # Load full info for candidates (limit to top 50 for speed)
        movies = self._load_movies_parallel(session, candidates[:50])

        if not movies:
            return None

        # Score and find the best
        best_movie = None
        best_score = float('-inf')

        for movie in movies:
            # Skip if somehow already rated
            if (movie.kinopoisk_id, movie.is_tv) in rated_ids:
                continue
            score = self.recommender.calculate_score(movie, session)
            if score > best_score:
                best_score = score
                best_movie = movie

        return best_movie

    def find_similar_movies(self, session: Session, source_movie: Movie) -> list[Movie]:
        """
        Find movies similar to the given movie using TMDB recommendations.
        Results are sorted by user preference score.
        """
        from database import get_cached_recommendations, save_cached_recommendations

        # Get TMDB recommendations for this movie
        cached_ids = get_cached_recommendations(session, source_movie.kinopoisk_id, source_movie.is_tv)

        if cached_ids is not None:
            candidates = [{"kinopoisk_id": rid, "is_tv": source_movie.is_tv} for rid in cached_ids]
        else:
            if source_movie.is_tv:
                recs = self.tmdb_api.get_recommendations_tv(source_movie.kinopoisk_id)
            else:
                recs = self.tmdb_api.get_recommendations_movie(source_movie.kinopoisk_id)

            # Cache for future use
            rec_ids = [r.get("kinopoisk_id") for r in recs if r.get("kinopoisk_id")]
            save_cached_recommendations(session, source_movie.kinopoisk_id, source_movie.is_tv, rec_ids)
            candidates = recs

        if not candidates:
            return []

        # Load full info
        movies = self._load_movies_parallel(session, candidates[:40])

        # Sort by user preference
        return self._sort_by_user_preference(session, movies)
