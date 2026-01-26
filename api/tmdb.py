import asyncio
from typing import Optional
import httpx


class TMDBAPI:
    """Async wrapper for The Movie Database (TMDB) API."""

    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"

    # Genre name to ID mapping (TMDB genre IDs for movies)
    MOVIE_GENRES = {
        "боевик": 28, "action": 28,
        "приключения": 12, "adventure": 12,
        "мультфильм": 16, "анимация": 16, "animation": 16,
        "комедия": 35, "comedy": 35,
        "криминал": 80, "crime": 80,
        "документальный": 99, "documentary": 99,
        "драма": 18, "drama": 18,
        "семейный": 10751, "family": 10751,
        "фэнтези": 14, "fantasy": 14,
        "история": 36, "history": 36,
        "ужасы": 27, "horror": 27,
        "музыка": 10402, "music": 10402,
        "детектив": 9648, "mystery": 9648,
        "мелодрама": 10749, "romance": 10749,
        "фантастика": 878, "science fiction": 878, "sci-fi": 878,
        "тв фильм": 10770, "tv movie": 10770,
        "триллер": 53, "thriller": 53,
        "военный": 10752, "war": 10752,
        "вестерн": 37, "western": 37,
    }

    # TV genre IDs
    TV_GENRES = {
        "боевик": 10759, "action": 10759, "приключения": 10759, "adventure": 10759,
        "мультфильм": 16, "анимация": 16, "animation": 16,
        "комедия": 35, "comedy": 35,
        "криминал": 80, "crime": 80,
        "документальный": 99, "documentary": 99,
        "драма": 18, "drama": 18,
        "семейный": 10751, "family": 10751,
        "детский": 10762, "kids": 10762,
        "детектив": 9648, "mystery": 9648,
        "новости": 10763, "news": 10763,
        "реалити": 10764, "reality": 10764,
        "фантастика": 10765, "sci-fi": 10765, "science fiction": 10765, "фэнтези": 10765, "fantasy": 10765,
        "мыльная опера": 10766, "soap": 10766,
        "ток-шоу": 10767, "talk": 10767,
        "военный": 10768, "war": 10768, "политика": 10768,
        "вестерн": 37, "western": 37,
    }

    GENRES = MOVIE_GENRES

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=15.0,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Make an async GET request to the API."""
        try:
            client = await self._get_client()
            if params is None:
                params = {}
            params["api_key"] = self.api_key
            params["language"] = "ru-RU"

            response = await client.get(f"{self.BASE_URL}{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

    async def search_movies(self, query: str, page: int = 1) -> list[dict]:
        """Search movies by title."""
        data = await self._get("/search/movie", {"query": query, "page": page})
        if not data or "results" not in data:
            return []
        return [self._parse_movie_basic(m) for m in data["results"]]

    async def search_person(self, query: str) -> list[dict]:
        """Search for a person by name."""
        data = await self._get("/search/person", {"query": query})
        if not data or "results" not in data:
            return []
        return data["results"]

    async def get_person_movies(self, person_id: int) -> list[dict]:
        """Get movies where person worked."""
        data = await self._get(f"/person/{person_id}/movie_credits")
        if not data:
            return []

        movies = []
        seen_ids = set()

        for movie in data.get("crew", []):
            if movie.get("id") not in seen_ids:
                seen_ids.add(movie.get("id"))
                movies.append(self._parse_movie_basic(movie))

        for movie in data.get("cast", []):
            if movie.get("id") not in seen_ids:
                seen_ids.add(movie.get("id"))
                movies.append(self._parse_movie_basic(movie))

        return movies

    async def get_movie_details(self, movie_id: int) -> Optional[dict]:
        """Get detailed information about a movie."""
        data = await self._get(f"/movie/{movie_id}", {"append_to_response": "external_ids"})
        if not data:
            return None

        external_ids = data.get("external_ids", {})
        data["imdb_id"] = external_ids.get("imdb_id") or data.get("imdb_id")
        return self._parse_movie_details(data)

    async def get_movie_credits(self, movie_id: int) -> dict:
        """Get cast and crew for a movie."""
        data = await self._get(f"/movie/{movie_id}/credits")
        if not data:
            return {"directors": [], "actors": []}
        return self._parse_credits(data)

    async def get_full_movie_info(self, movie_id: int) -> Optional[dict]:
        """Get complete movie information including credits in ONE request."""
        data = await self._get(f"/movie/{movie_id}", {"append_to_response": "credits,external_ids"})
        if not data:
            return None

        external_ids = data.get("external_ids", {})
        data["imdb_id"] = external_ids.get("imdb_id") or data.get("imdb_id")

        details = self._parse_movie_details(data)
        credits_data = data.get("credits", {})
        credits = self._parse_credits(credits_data)
        details["directors"] = credits["directors"]
        details["actors"] = credits["actors"][:10]

        return details

    # ========== TV SHOW METHODS ==========

    async def search_tv(self, query: str, page: int = 1) -> list[dict]:
        """Search TV shows by title."""
        data = await self._get("/search/tv", {"query": query, "page": page})
        if not data or "results" not in data:
            return []
        return [self._parse_tv_basic(tv) for tv in data["results"]]

    async def get_tv_details(self, tv_id: int) -> Optional[dict]:
        """Get detailed information about a TV show."""
        data = await self._get(f"/tv/{tv_id}", {"append_to_response": "external_ids"})
        if not data:
            return None

        external_ids = data.get("external_ids", {})
        data["imdb_id"] = external_ids.get("imdb_id")
        return self._parse_tv_details(data)

    async def get_tv_credits(self, tv_id: int) -> dict:
        """Get cast and crew for a TV show."""
        data = await self._get(f"/tv/{tv_id}/credits")
        if not data:
            return {"directors": [], "actors": []}
        return self._parse_tv_credits(data)

    async def get_full_tv_info(self, tv_id: int) -> Optional[dict]:
        """Get complete TV show information including credits in ONE request."""
        data = await self._get(f"/tv/{tv_id}", {"append_to_response": "credits,external_ids"})
        if not data:
            return None

        external_ids = data.get("external_ids", {})
        data["imdb_id"] = external_ids.get("imdb_id")

        details = self._parse_tv_details(data)
        credits_data = data.get("credits", {})
        credits = self._parse_tv_credits(credits_data)
        details["directors"] = credits["directors"]
        details["actors"] = credits["actors"][:10]

        return details

    async def get_person_tv(self, person_id: int) -> list[dict]:
        """Get TV shows where person worked."""
        data = await self._get(f"/person/{person_id}/tv_credits")
        if not data:
            return []

        shows = []
        seen_ids = set()

        for show in data.get("crew", []):
            if show.get("id") not in seen_ids:
                seen_ids.add(show.get("id"))
                shows.append(self._parse_tv_basic(show))

        for show in data.get("cast", []):
            if show.get("id") not in seen_ids:
                seen_ids.add(show.get("id"))
                shows.append(self._parse_tv_basic(show))

        return shows

    async def discover_tv_by_genre(self, genre_ids: list[int], page: int = 1) -> list[dict]:
        """Discover TV shows by genre IDs."""
        if not genre_ids:
            return []

        genres_str = ",".join(str(g) for g in genre_ids)
        data = await self._get("/discover/tv", {
            "with_genres": genres_str,
            "sort_by": "popularity.desc",
            "page": page,
        })

        if not data or "results" not in data:
            return []
        return [self._parse_tv_basic(tv) for tv in data["results"]]

    # ========== MOVIE METHODS ==========

    async def discover_by_genre(self, genre_ids: list[int], page: int = 1) -> list[dict]:
        """Discover movies by genre IDs."""
        if not genre_ids:
            return []

        genres_str = ",".join(str(g) for g in genre_ids)
        data = await self._get("/discover/movie", {
            "with_genres": genres_str,
            "sort_by": "popularity.desc",
            "page": page,
        })

        if not data or "results" not in data:
            return []
        return [self._parse_movie_basic(m) for m in data["results"]]

    async def get_recommendations_movie(self, movie_id: int) -> list[dict]:
        """Get movie recommendations."""
        data = await self._get(f"/movie/{movie_id}/recommendations")
        if not data or "results" not in data:
            return []
        return [self._parse_movie_basic(m) for m in data["results"][:10]]

    async def get_recommendations_tv(self, tv_id: int) -> list[dict]:
        """Get TV recommendations."""
        data = await self._get(f"/tv/{tv_id}/recommendations")
        if not data or "results" not in data:
            return []
        return [self._parse_tv_basic(tv) for tv in data["results"][:10]]

    async def search_keywords(self, query: str) -> list[dict]:
        """Search for TMDB keywords."""
        data = await self._get("/search/keyword", {"query": query})
        if not data or "results" not in data:
            return []
        return data["results"]

    async def discover_by_keywords(self, keyword_ids: list[int], page: int = 1) -> list[dict]:
        """Discover movies by TMDB keyword IDs."""
        if not keyword_ids:
            return []
        keywords_str = "|".join(str(k) for k in keyword_ids)
        data = await self._get("/discover/movie", {
            "with_keywords": keywords_str,
            "sort_by": "popularity.desc",
            "page": page,
        })
        if not data or "results" not in data:
            return []
        return [self._parse_movie_basic(m) for m in data["results"]]

    async def discover_tv_by_keywords(self, keyword_ids: list[int], page: int = 1) -> list[dict]:
        """Discover TV shows by TMDB keyword IDs."""
        if not keyword_ids:
            return []
        keywords_str = "|".join(str(k) for k in keyword_ids)
        data = await self._get("/discover/tv", {
            "with_keywords": keywords_str,
            "sort_by": "popularity.desc",
            "page": page,
        })
        if not data or "results" not in data:
            return []
        return [self._parse_tv_basic(tv) for tv in data["results"]]

    async def search_by_keyword(self, keyword: str, page: int = 1) -> list[dict]:
        """Search movies AND TV shows by title, person name, genre."""
        seen_movie_ids = set()
        seen_tv_ids = set()
        results = []

        keyword_lower = keyword.lower()

        # Prepare all tasks
        tasks = []

        # Genre searches
        movie_genre_id = self.MOVIE_GENRES.get(keyword_lower)
        if movie_genre_id:
            tasks.append(("discover_movie", self.discover_by_genre([movie_genre_id], page)))

        tv_genre_id = self.TV_GENRES.get(keyword_lower)
        if tv_genre_id:
            tasks.append(("discover_tv", self.discover_tv_by_genre([tv_genre_id], page)))

        # Title searches
        tasks.append(("search_movie", self.search_movies(keyword, page)))
        tasks.append(("search_tv", self.search_tv(keyword, page)))

        # Person search
        tasks.append(("search_person", self.search_person(keyword)))

        # Execute all tasks concurrently
        task_results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)

        person_ids = []
        for (task_type, _), result in zip(tasks, task_results):
            if isinstance(result, Exception):
                continue

            if task_type == "search_person":
                person_ids = [p.get("id") for p in result[:2] if p.get("id")]
            else:
                is_tv = task_type in ("discover_tv", "search_tv")
                target_set = seen_tv_ids if is_tv else seen_movie_ids
                for item in result:
                    tmdb_id = item.get("kinopoisk_id")
                    if tmdb_id and tmdb_id not in target_set:
                        target_set.add(tmdb_id)
                        results.append(item)

        # Fetch person filmographies concurrently
        if person_ids:
            person_tasks = []
            for pid in person_ids:
                person_tasks.append(("movie", self.get_person_movies(pid)))
                person_tasks.append(("tv", self.get_person_tv(pid)))

            person_results = await asyncio.gather(*[t[1] for t in person_tasks], return_exceptions=True)

            for (task_type, _), result in zip(person_tasks, person_results):
                if isinstance(result, Exception):
                    continue
                items = result[:10]
                is_tv = task_type == "tv"
                target_set = seen_tv_ids if is_tv else seen_movie_ids
                for item in items:
                    tmdb_id = item.get("kinopoisk_id")
                    if tmdb_id and tmdb_id not in target_set:
                        target_set.add(tmdb_id)
                        results.append(item)

        return results

    # ========== PARSERS ==========

    def _parse_tv_basic(self, tv: dict) -> dict:
        """Parse basic TV show info from search results."""
        poster_path = tv.get("poster_path")
        poster_url = f"{self.IMAGE_BASE_URL}{poster_path}" if poster_path else None

        first_air_date = tv.get("first_air_date") or ""
        year = int(first_air_date[:4]) if len(first_air_date) >= 4 else None

        return {
            "kinopoisk_id": tv.get("id"),
            "is_tv": True,
            "title": tv.get("name") or tv.get("original_name") or "Unknown",
            "title_original": tv.get("original_name"),
            "year": year,
            "genres": "",
            "poster_url": poster_url,
            "kp_rating": tv.get("vote_average"),
            "description": tv.get("overview"),
        }

    def _parse_tv_details(self, tv: dict) -> dict:
        """Parse detailed TV show information."""
        poster_path = tv.get("poster_path")
        poster_url = f"{self.IMAGE_BASE_URL}{poster_path}" if poster_path else None

        genres = [g.get("name", "") for g in tv.get("genres", [])]

        year = None
        if tv.get("first_air_date"):
            try:
                year = int(tv.get("first_air_date", "")[:4])
            except (ValueError, TypeError):
                pass

        return {
            "kinopoisk_id": tv.get("id"),
            "is_tv": True,
            "title": tv.get("name") or tv.get("original_name") or "Unknown",
            "title_original": tv.get("original_name"),
            "year": year,
            "genres": ", ".join(genres),
            "poster_url": poster_url,
            "tmdb_rating": tv.get("vote_average"),
            "imdb_id": tv.get("imdb_id"),
            "description": tv.get("overview"),
        }

    def _parse_tv_credits(self, credits: dict) -> dict:
        """Parse TV credits to extract creator and actors with TMDB IDs."""
        directors = []
        actors = []

        for person in credits.get("crew", []):
            job = person.get("job", "")
            if job in ("Executive Producer", "Creator"):
                directors.append({
                    "tmdb_id": person.get("id"),
                    "name": person.get("name"),
                })

        for person in credits.get("cast", []):
            tmdb_id = person.get("id")
            name = person.get("name")
            if tmdb_id and name:
                actors.append({
                    "tmdb_id": tmdb_id,
                    "name": name,
                })

        return {"directors": directors, "actors": actors}

    def _parse_movie_basic(self, movie: dict) -> dict:
        """Parse basic movie info from search results."""
        poster_path = movie.get("poster_path")
        poster_url = f"{self.IMAGE_BASE_URL}{poster_path}" if poster_path else None

        release_date = movie.get("release_date") or ""
        year = int(release_date[:4]) if len(release_date) >= 4 else None

        return {
            "kinopoisk_id": movie.get("id"),
            "is_tv": False,
            "title": movie.get("title") or movie.get("original_title") or "Unknown",
            "title_original": movie.get("original_title"),
            "year": year,
            "genres": "",
            "poster_url": poster_url,
            "kp_rating": movie.get("vote_average"),
            "description": movie.get("overview"),
        }

    def _parse_movie_details(self, movie: dict) -> dict:
        """Parse detailed movie information."""
        poster_path = movie.get("poster_path")
        poster_url = f"{self.IMAGE_BASE_URL}{poster_path}" if poster_path else None

        genres = [g.get("name", "") for g in movie.get("genres", [])]

        year = None
        if movie.get("release_date"):
            try:
                year = int(movie.get("release_date", "")[:4])
            except (ValueError, TypeError):
                pass

        return {
            "kinopoisk_id": movie.get("id"),
            "is_tv": False,
            "title": movie.get("title") or movie.get("original_title") or "Unknown",
            "title_original": movie.get("original_title"),
            "year": year,
            "genres": ", ".join(genres),
            "poster_url": poster_url,
            "tmdb_rating": movie.get("vote_average"),
            "imdb_id": movie.get("imdb_id"),
            "description": movie.get("overview"),
        }

    def _parse_credits(self, credits: dict) -> dict:
        """Parse credits to extract director and actors with TMDB IDs."""
        directors = []
        actors = []

        for person in credits.get("crew", []):
            if person.get("job") == "Director":
                directors.append({
                    "tmdb_id": person.get("id"),
                    "name": person.get("name"),
                })

        for person in credits.get("cast", []):
            tmdb_id = person.get("id")
            name = person.get("name")
            if tmdb_id and name:
                actors.append({
                    "tmdb_id": tmdb_id,
                    "name": name,
                })

        return {"directors": directors, "actors": actors}
