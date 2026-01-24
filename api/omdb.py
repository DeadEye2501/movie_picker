from typing import Optional
import httpx


class OMDBAPI:
    """Wrapper for OMDB API to get IMDB, Rotten Tomatoes, Metacritic ratings."""

    BASE_URL = "https://www.omdbapi.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.Client(timeout=10.0)

    def get_ratings_by_imdb_id(self, imdb_id: str) -> dict:
        """Get ratings from OMDB by IMDB ID."""
        if not imdb_id:
            return {}

        try:
            response = self._client.get(
                self.BASE_URL,
                params={"i": imdb_id, "apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if data.get("Response") == "False":
                return {}

            return self._parse_ratings(data)
        except Exception:
            return {}

    def _parse_ratings(self, data: dict) -> dict:
        """Parse ratings from OMDB response."""
        ratings = {}

        # IMDB rating
        imdb_rating = data.get("imdbRating")
        if imdb_rating and imdb_rating != "N/A":
            try:
                ratings["imdb"] = float(imdb_rating)
            except ValueError:
                pass

        # Parse Ratings array for Rotten Tomatoes and Metacritic
        for rating in data.get("Ratings", []):
            source = rating.get("Source", "")
            value = rating.get("Value", "")

            if "Rotten Tomatoes" in source:
                try:
                    ratings["rotten_tomatoes"] = int(value.replace("%", ""))
                except ValueError:
                    pass
            elif "Metacritic" in source:
                try:
                    ratings["metacritic"] = int(value.split("/")[0])
                except (ValueError, IndexError):
                    pass

        return ratings
