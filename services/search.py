import re
import asyncio
from typing import Optional, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from api import TMDBAPI, OMDBAPI, KinopoiskAPI, MDBListAPI
from database import get_movie_by_kp_id, save_movie, search_local_movies, get_all_user_ratings
from database.models import Movie
from .recommender import RecommenderService


class SearchService:
    """Async service for searching and retrieving movies."""

    # Movie to TV genre ID mapping (TMDB IDs)
    MOVIE_TO_TV_GENRE_MAP = {
        28: 10759, 12: 10759, 16: 16, 35: 35, 80: 80, 99: 99, 18: 18,
        10751: 10751, 14: 10765, 27: None, 10402: None, 9648: 9648,
        10749: None, 878: 10765, 53: None, 10752: 10768, 37: 37,
    }

    def __init__(self, tmdb_api: TMDBAPI, omdb_api: OMDBAPI, kp_api: KinopoiskAPI, mdblist_api: MDBListAPI, recommender: RecommenderService):
        self.tmdb_api = tmdb_api
        self.omdb_api = omdb_api
        self.kp_api = kp_api
        self.mdblist_api = mdblist_api
        self.recommender = recommender

    async def close(self):
        """Close the search service (clears any internal caches)."""
        self.recommender.clear_cache()

    async def search_movies(self, session: AsyncSession, query: str, page: int = 1, genres: list[int] = None, skip_ratings: bool = False, start_page: int = 1, num_pages: int = 3) -> list[Movie]:
        """Search for movies AND TV shows by keyword and/or genres."""
        query_words = [w.lower() for w in query.split() if w.strip()]
        genres = genres or []

        if not query_words and not genres:
            return []

        seen_movie_ids = set()
        seen_tv_ids = set()
        all_movies = []
        all_search_results = []

        from database import get_rated_movies, get_cached_recommendations, save_cached_recommendations

        # 1. Get recommendations from user's rated movies (only on first page)
        if start_page > 1:
            rated_movies = []
        else:
            rated_movies = await get_rated_movies(session, min_rating=6)
        rated_movies = rated_movies[:10]
        uncached_rated = []

        for rated in rated_movies:
            cached_ids = await get_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv)
            if cached_ids is not None:
                for rec_id in cached_ids[:10]:
                    target_set = seen_tv_ids if rated.is_tv else seen_movie_ids
                    if rec_id and rec_id not in target_set:
                        target_set.add(rec_id)
                        all_search_results.append({"kinopoisk_id": rec_id, "is_tv": rated.is_tv})
            else:
                uncached_rated.append(rated)

        # Fetch uncached recommendations concurrently
        if uncached_rated:
            tasks = []
            for r in uncached_rated:
                if r.is_tv:
                    tasks.append(self.tmdb_api.get_recommendations_tv(r.kinopoisk_id))
                else:
                    tasks.append(self.tmdb_api.get_recommendations_movie(r.kinopoisk_id))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for rated, result in zip(uncached_rated, results):
                if isinstance(result, Exception):
                    continue
                rec_ids = [r.get("kinopoisk_id") for r in result if r.get("kinopoisk_id")]
                await save_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv, rec_ids)
                for rec in result[:10]:
                    rec_id = rec.get("kinopoisk_id")
                    is_tv = rec.get("is_tv", False)
                    target_set = seen_tv_ids if is_tv else seen_movie_ids
                    if rec_id and rec_id not in target_set:
                        target_set.add(rec_id)
                        all_search_results.append(rec)

        # 2. Parallel API calls for discover and search
        api_tasks = []
        task_info = []

        end_page = start_page + num_pages
        if genres:
            tv_genres = self._map_movie_genres_to_tv(genres)
            for pg in range(start_page, end_page):
                api_tasks.append(self.tmdb_api.discover_by_genre(genres, pg))
                task_info.append(("discover_movie", genres, pg))
                if tv_genres:
                    api_tasks.append(self.tmdb_api.discover_tv_by_genre(tv_genres, pg))
                    task_info.append(("discover_tv", tv_genres, pg))

        if query_words:
            for word in query_words:
                for pg in range(start_page, end_page):
                    api_tasks.append(self.tmdb_api.search_by_keyword(word, pg))
                    task_info.append(("search", word, pg))

        if api_tasks:
            results = await asyncio.gather(*api_tasks, return_exceptions=True)
            for info, result in zip(task_info, results):
                if isinstance(result, Exception):
                    continue
                for item in result:
                    tmdb_id = item.get("kinopoisk_id")
                    is_tv = item.get("is_tv", False)
                    target_set = seen_tv_ids if is_tv else seen_movie_ids
                    if tmdb_id and tmdb_id not in target_set:
                        target_set.add(tmdb_id)
                        all_search_results.append(item)

        # 3. Search local database (only on first page)
        if query_words and start_page == 1:
            for word in query_words:
                local_movies = await search_local_movies(session, word)
                for movie in local_movies:
                    target_set = seen_tv_ids if movie.is_tv else seen_movie_ids
                    if movie.kinopoisk_id not in target_set:
                        target_set.add(movie.kinopoisk_id)
                        all_movies.append(movie)

        # 4. Load full info for API results
        if all_search_results:
            api_movies = await self._load_movies_parallel(session, all_search_results, skip_ratings=skip_ratings)
            all_movies.extend(api_movies)

        # 5. Filter by ALL query words
        if query_words:
            filtered_movies = [m for m in all_movies if self._matches_all_words(m, query_words)]
        else:
            filtered_movies = all_movies

        # 6. Filter by selected genres
        if genres:
            filtered_movies = [m for m in filtered_movies if self._matches_genres(m, genres)]

        sorted_movies = await self._sort_by_user_preference(session, filtered_movies)
        return sorted_movies

    def _map_movie_genres_to_tv(self, movie_genre_ids: list[int]) -> list[int]:
        """Map movie genre IDs to TV genre IDs."""
        tv_genres = []
        seen = set()
        for gid in movie_genre_ids:
            tv_gid = self.MOVIE_TO_TV_GENRE_MAP.get(gid)
            if tv_gid and tv_gid not in seen:
                seen.add(tv_gid)
                tv_genres.append(tv_gid)
        return tv_genres

    async def _load_movies_parallel(self, session: AsyncSession, search_results: list[dict], skip_ratings: bool = False) -> list[Movie]:
        """Load movie/TV details concurrently."""
        movies = []
        to_load = []

        for result in search_results:
            kp_id = result.get("kinopoisk_id")
            is_tv = result.get("is_tv", False)
            if not kp_id:
                continue

            existing_movie = await get_movie_by_kp_id(session, kp_id, is_tv)
            if existing_movie and existing_movie.director_list:
                movies.append(existing_movie)
            else:
                to_load.append((kp_id, is_tv))

        if to_load:
            tasks = [self._load_single_item(kp_id, is_tv, skip_ratings) for kp_id, is_tv in to_load]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception) or result is None:
                    continue
                try:
                    movie = await save_movie(session, result)
                    movies.append(movie)
                except Exception:
                    pass

        return movies

    async def fetch_missing_ratings(self, session: AsyncSession, movies: list[Movie], on_movie_updated: Optional[Callable] = None):
        """Fetch missing ratings for movies without blocking the database."""
        has_rating_api = self.mdblist_api or self.omdb_api

        # 1. Collect movie info we need (without holding session during HTTP calls)
        movies_info = []
        for m in movies:
            movies_info.append({
                "kinopoisk_id": m.kinopoisk_id,
                "is_tv": m.is_tv,
                "imdb_id": m.imdb_id,
                "imdb_rating": m.imdb_rating,
                "kp_rating": m.kp_rating,
                "title": m.title,
                "title_original": m.title_original,
                "year": m.year,
            })

        # 2. Fetch all external ratings concurrently (no DB access needed)
        external_results = {}
        if has_rating_api:
            needing_external = [m for m in movies_info if m["imdb_rating"] is None]
            if needing_external:
                tasks = [self._fetch_external_ratings_by_info(m) for m in needing_external]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for m, result in zip(needing_external, results):
                    if not isinstance(result, Exception) and result:
                        external_results[m["kinopoisk_id"]] = result

        # 3. Fetch all KP ratings concurrently (no DB access needed)
        kp_results = {}
        if self.kp_api:
            needing_kp = [m for m in movies_info if m["kp_rating"] is None]
            if needing_kp:
                tasks = [self._fetch_kp_rating(m) for m in needing_kp]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for m, result in zip(needing_kp, results):
                    if not isinstance(result, Exception) and result is not None:
                        kp_results[m["kinopoisk_id"]] = result

        # 4. Now update the database - quick operations
        for m_info in movies_info:
            kp_id = m_info["kinopoisk_id"]
            ext_data = external_results.get(kp_id, {})
            kp_data = kp_results.get(kp_id)

            if not ext_data and kp_data is None:
                continue

            # Re-fetch movie in current session for update
            movie = await get_movie_by_kp_id(session, kp_id, m_info["is_tv"])
            if not movie:
                continue

            updated = False
            if ext_data.get("imdb") is not None:
                movie.imdb_rating = ext_data["imdb"]
                updated = True
            if ext_data.get("rotten_tomatoes") is not None:
                movie.rotten_tomatoes = ext_data["rotten_tomatoes"]
                updated = True
            if ext_data.get("metacritic") is not None:
                movie.metacritic = ext_data["metacritic"]
                updated = True
            if kp_data is not None:
                movie.kp_rating = kp_data
                updated = True

            if updated:
                await session.commit()
                if on_movie_updated:
                    on_movie_updated(movie)

    async def _fetch_external_ratings_by_info(self, movie_info: dict) -> dict:
        """Fetch external ratings using movie info dict."""
        ratings = {}

        if self.mdblist_api:
            ratings = await self.mdblist_api.get_ratings_by_tmdb_id(movie_info["kinopoisk_id"], movie_info["is_tv"])

        if not ratings and self.omdb_api and movie_info.get("imdb_id"):
            omdb_ratings = await self.omdb_api.get_ratings_by_imdb_id(movie_info["imdb_id"])
            ratings = {
                "imdb": omdb_ratings.get("imdb"),
                "rotten_tomatoes": omdb_ratings.get("rotten_tomatoes"),
                "metacritic": omdb_ratings.get("metacritic"),
            }

        return ratings


    async def _load_single_item(self, item_id: int, is_tv: bool, skip_ratings: bool = False) -> Optional[dict]:
        """Load a single movie or TV show with all ratings."""
        if is_tv:
            full_info = await self.tmdb_api.get_full_tv_info(item_id)
        else:
            full_info = await self.tmdb_api.get_full_movie_info(item_id)

        if not full_info:
            return None

        if not skip_ratings:
            await self._fetch_external_ratings(full_info, item_id, is_tv)

            if self.kp_api:
                kp_rating = await self._fetch_kp_rating(full_info)
                if kp_rating is not None:
                    full_info["kp_rating"] = kp_rating

        return full_info

    async def _fetch_external_ratings(self, full_info: dict, tmdb_id: int, is_tv: bool):
        """Fetch IMDB/RT/MC ratings."""
        ratings = {}

        if self.mdblist_api:
            ratings = await self.mdblist_api.get_ratings_by_tmdb_id(tmdb_id, is_tv)

        if not ratings and self.omdb_api and full_info.get("imdb_id"):
            omdb_ratings = await self.omdb_api.get_ratings_by_imdb_id(full_info["imdb_id"])
            ratings = {
                "imdb": omdb_ratings.get("imdb"),
                "rotten_tomatoes": omdb_ratings.get("rotten_tomatoes"),
                "metacritic": omdb_ratings.get("metacritic"),
            }

        if ratings.get("imdb") is not None:
            full_info["imdb_rating"] = ratings["imdb"]
        if ratings.get("rotten_tomatoes") is not None:
            full_info["rotten_tomatoes"] = ratings["rotten_tomatoes"]
        if ratings.get("metacritic") is not None:
            full_info["metacritic"] = ratings["metacritic"]

    async def _fetch_kp_rating(self, movie_info: dict) -> Optional[float]:
        """Fetch Kinopoisk rating by searching for movie by title and year."""
        if not self.kp_api:
            return None

        title = movie_info.get("title") or movie_info.get("title_original")
        title_original = movie_info.get("title_original")
        year = movie_info.get("year")

        if not title:
            return None

        def strip_parentheses(t: str) -> str:
            return re.sub(r'\s*\([^)]*\)', '', t).strip()

        def normalize_title(t: str) -> str:
            t = strip_parentheses(t)
            t = re.sub(r'[:\-–—\'\"""«»]', ' ', t.lower())
            t = re.sub(r'\s+', ' ', t).strip()
            return t

        def titles_match(t1: str, t2: str) -> bool:
            n1, n2 = normalize_title(t1), normalize_title(t2)
            if n1 == n2 or n1 in n2 or n2 in n1:
                return True
            words1 = {w for w in n1.split() if len(w) >= 3}
            words2 = {w for w in n2.split() if len(w) >= 3}
            if not words1 or not words2:
                return n1 == n2
            common = words1 & words2
            min_len = min(len(words1), len(words2))
            return len(common) >= max(1, min_len * 0.6)

        def years_match(y1: Optional[int], y2: Optional[int]) -> bool:
            if y1 is None or y2 is None:
                return True
            return abs(y1 - y2) <= 1

        try:
            all_results = []

            search_title = strip_parentheses(title)
            results = await self.kp_api.search_by_keyword(search_title)
            all_results.extend(results[:15])

            if title_original:
                search_orig = strip_parentheses(title_original)
                if search_orig.lower() != search_title.lower():
                    results_orig = await self.kp_api.search_by_keyword(search_orig)
                    for r in results_orig[:15]:
                        if r.get("kinopoisk_id") not in {x.get("kinopoisk_id") for x in all_results}:
                            all_results.append(r)

            for result in all_results:
                result_title = result.get("title") or ""
                result_title_orig = result.get("title_original") or ""
                result_year = result.get("year")

                title_ok = titles_match(title, result_title) or (
                    title_original and titles_match(title_original, result_title_orig)
                )
                year_ok = years_match(year, result_year)

                if title_ok and year_ok:
                    rating = result.get("kp_rating")
                    if rating:
                        return rating

        except Exception:
            pass

        return None

    def _matches_all_words(self, movie: Movie, query_words: list[str]) -> bool:
        """Check if movie matches ALL query words."""
        searchable_text = self._get_searchable_text(movie)
        for word in query_words:
            if word not in searchable_text:
                return False
        return True

    def _matches_genres(self, movie: Movie, genre_ids: list[int]) -> bool:
        """Check if movie matches ALL selected genres."""
        if not movie.genre_list:
            return False

        movie_genre_names = {g.name.lower() for g in movie.genre_list}

        genre_names = {
            28: "боевик", 12: "приключения", 16: "мультфильм", 35: "комедия",
            80: "криминал", 99: "документальный", 18: "драма", 10751: "семейный",
            14: "фэнтези", 36: "история", 27: "ужасы", 10402: "музыка",
            9648: "детектив", 10749: "мелодрама", 878: "фантастика",
            10770: "тв фильм", 53: "триллер", 10752: "военный", 37: "вестерн",
            10759: "боевик", 10765: "фантастика", 10762: "детский",
            10763: "новости", 10764: "реалити", 10766: "мыльная опера",
            10767: "ток-шоу", 10768: "военный",
        }

        for genre_id in genre_ids:
            genre_name = genre_names.get(genre_id, "")
            if genre_name and genre_name not in movie_genre_names:
                return False

        return True

    def _get_searchable_text(self, movie: Movie) -> str:
        """Get all searchable text from movie."""
        parts = []
        if movie.title:
            parts.append(movie.title.lower())
        if movie.title_original:
            parts.append(movie.title_original.lower())
        if movie.genre_list:
            parts.append(movie.genres_display.lower())
        if movie.director_list:
            parts.append(movie.directors_display.lower())
        if movie.actor_list:
            parts.append(movie.actors_display.lower())
        if movie.description:
            parts.append(movie.description.lower())
        return " ".join(parts)

    async def _sort_by_user_preference(self, session: AsyncSession, movies: list[Movie]) -> list[Movie]:
        """Sort movies by personal recommendation score."""
        if not movies:
            return movies

        # Pre-load all user ratings ONCE for the entire sorting operation
        cached_ratings = await get_all_user_ratings(session)

        if not await self.recommender.has_user_ratings(session, cached_ratings):
            return sorted(movies, key=lambda m: m.tmdb_rating or 0, reverse=True)

        scored_movies = []
        for movie in movies:
            score = await self.recommender.calculate_score(movie, session, cached_ratings)
            scored_movies.append((movie, score))

        scored_movies.sort(key=lambda x: x[1], reverse=True)
        return [movie for movie, _ in scored_movies]

    async def find_magic_recommendation(self, session: AsyncSession) -> Optional[Movie]:
        """Find the single best unwatched movie based on user's preferences."""
        from database import get_rated_movies, get_cached_recommendations, save_cached_recommendations, get_wishlist_movie_ids

        rated_movies = await get_rated_movies(session, min_rating=6)
        if not rated_movies:
            return None

        # Pre-load all user ratings ONCE (used for filtering and scoring)
        cached_ratings = await get_all_user_ratings(session)
        rated_ids = {(ur.movie.kinopoisk_id, ur.movie.is_tv) for ur in cached_ratings}
        
        # Get wishlist movie IDs to exclude them from recommendations
        wishlist_ids = await get_wishlist_movie_ids(session)

        seen_ids = set()
        candidates = []
        uncached = []

        for rated in rated_movies[:20]:
            cached_ids = await get_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv)
            if cached_ids is not None:
                for rec_id in cached_ids:
                    key = (rec_id, rated.is_tv)
                    if key not in rated_ids and key not in seen_ids:
                        seen_ids.add(key)
                        candidates.append({"kinopoisk_id": rec_id, "is_tv": rated.is_tv})
            else:
                uncached.append(rated)

        if uncached:
            tasks = []
            for r in uncached:
                if r.is_tv:
                    tasks.append(self.tmdb_api.get_recommendations_tv(r.kinopoisk_id))
                else:
                    tasks.append(self.tmdb_api.get_recommendations_movie(r.kinopoisk_id))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for rated, result in zip(uncached, results):
                if isinstance(result, Exception):
                    continue
                rec_ids = [r.get("kinopoisk_id") for r in result if r.get("kinopoisk_id")]
                await save_cached_recommendations(session, rated.kinopoisk_id, rated.is_tv, rec_ids)
                for rec in result:
                    rec_id = rec.get("kinopoisk_id")
                    is_tv = rec.get("is_tv", False)
                    key = (rec_id, is_tv)
                    if key not in rated_ids and key not in seen_ids:
                        seen_ids.add(key)
                        candidates.append(rec)

        if not candidates:
            return None

        movies = await self._load_movies_parallel(session, candidates[:50])

        if not movies:
            return None

        best_movie = None
        best_score = float('-inf')

        for movie in movies:
            if (movie.kinopoisk_id, movie.is_tv) in rated_ids:
                continue
            # Skip movies that are in wishlist
            if movie.id in wishlist_ids:
                continue
            score = await self.recommender.calculate_score(movie, session, cached_ratings)
            if score > best_score:
                best_score = score
                best_movie = movie

        return best_movie

    async def find_similar_movies(self, session: AsyncSession, source_movie: Movie) -> list[Movie]:
        """Find movies similar to the given movie using TMDB recommendations."""
        from database import get_cached_recommendations, save_cached_recommendations

        cached_ids = await get_cached_recommendations(session, source_movie.kinopoisk_id, source_movie.is_tv)

        if cached_ids is not None:
            candidates = [{"kinopoisk_id": rid, "is_tv": source_movie.is_tv} for rid in cached_ids]
        else:
            if source_movie.is_tv:
                recs = await self.tmdb_api.get_recommendations_tv(source_movie.kinopoisk_id)
            else:
                recs = await self.tmdb_api.get_recommendations_movie(source_movie.kinopoisk_id)

            rec_ids = [r.get("kinopoisk_id") for r in recs if r.get("kinopoisk_id")]
            await save_cached_recommendations(session, source_movie.kinopoisk_id, source_movie.is_tv, rec_ids)
            candidates = recs

        if not candidates:
            return []

        movies = await self._load_movies_parallel(session, candidates[:40])
        return await self._sort_by_user_preference(session, movies)
