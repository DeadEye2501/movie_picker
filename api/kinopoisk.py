from typing import Optional
import httpx


class KinopoiskAPI:
    """Wrapper for Kinopoisk Unofficial API using httpx."""

    BASE_URL = "https://kinopoiskapiunofficial.tech/api"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        }

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Make a GET request to the API."""
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(
                    f"{self.BASE_URL}{endpoint}",
                    headers=self.headers,
                    params=params,
                )
                response.raise_for_status()
                return response.json()
        except Exception:
            return None

    def search_by_keyword(self, keyword: str, page: int = 1) -> list[dict]:
        """Search films by keyword."""
        data = self._get("/v2.1/films/search-by-keyword", {"keyword": keyword, "page": page})

        if not data or "films" not in data:
            return []

        films = []
        for film in data["films"]:
            films.append(self._parse_search_result(film))

        return films

    def get_film_details(self, film_id: int) -> Optional[dict]:
        """Get detailed information about a film."""
        data = self._get(f"/v2.2/films/{film_id}")

        if not data:
            return None

        return self._parse_film_details(data)

    def get_staff(self, film_id: int) -> dict:
        """Get staff information (actors, directors) for a film."""
        data = self._get(f"/v1/staff", {"filmId": film_id})

        if not data:
            return {"director": None, "actors": []}

        return self._parse_staff(data)

    def get_full_movie_info(self, film_id: int) -> Optional[dict]:
        """Get complete movie information including staff."""
        details = self.get_film_details(film_id)
        if details is None:
            return None

        staff = self.get_staff(film_id)
        details["director"] = staff["director"]
        details["actors"] = ", ".join(staff["actors"][:10])

        return details

    def _parse_search_result(self, film: dict) -> dict:
        """Parse a film from search results."""
        genres = []
        if "genres" in film and film["genres"]:
            genres = [g.get("genre", "") for g in film["genres"] if g.get("genre")]

        film_id = film.get("filmId") or film.get("kinopoiskId")

        return {
            "kinopoisk_id": film_id,
            "title": film.get("nameRu") or film.get("nameEn") or "Unknown",
            "title_original": film.get("nameEn"),
            "year": self._parse_year(film.get("year")),
            "genres": ", ".join(genres),
            "poster_url": film.get("posterUrl") or film.get("posterUrlPreview"),
            "kp_rating": self._parse_rating(film.get("rating")),
            "description": film.get("description"),
        }

    def _parse_film_details(self, film: dict) -> dict:
        """Parse detailed film information."""
        genres = []
        if "genres" in film and film["genres"]:
            genres = [g.get("genre", "") for g in film["genres"] if g.get("genre")]

        return {
            "kinopoisk_id": film.get("kinopoiskId"),
            "title": film.get("nameRu") or film.get("nameEn") or "Unknown",
            "title_original": film.get("nameEn") or film.get("nameOriginal"),
            "year": film.get("year"),
            "genres": ", ".join(genres),
            "poster_url": film.get("posterUrl") or film.get("posterUrlPreview"),
            "kp_rating": film.get("ratingKinopoisk"),
            "description": film.get("description") or film.get("shortDescription"),
        }

    def _parse_staff(self, staff_list: list) -> dict:
        """Parse staff information to extract director and actors."""
        director = None
        actors = []

        for person in staff_list:
            profession = person.get("professionKey", "")
            name = person.get("nameRu") or person.get("nameEn") or ""

            if profession == "DIRECTOR" and director is None:
                director = name
            elif profession == "ACTOR":
                actors.append(name)

        return {"director": director, "actors": actors}

    def _parse_year(self, year_value) -> Optional[int]:
        """Parse year from various formats."""
        if year_value is None:
            return None
        if isinstance(year_value, int):
            return year_value
        if isinstance(year_value, str):
            try:
                return int(year_value.split("-")[0])
            except (ValueError, IndexError):
                return None
        return None

    def _parse_rating(self, rating_value) -> Optional[float]:
        """Parse rating from various formats."""
        if rating_value is None:
            return None
        if isinstance(rating_value, (int, float)):
            return float(rating_value)
        if isinstance(rating_value, str):
            try:
                cleaned = rating_value.replace("%", "").strip()
                return float(cleaned)
            except ValueError:
                return None
        return None
