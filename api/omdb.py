from typing import Optional
import httpx


class OMDBAPI:
    """Async wrapper for OMDB API to get IMDB, Rotten Tomatoes, Metacritic ratings."""

    BASE_URL = "https://www.omdbapi.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client: Optional[httpx.AsyncClient] = None
        self._disabled = False

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def get_ratings_by_imdb_id(self, imdb_id: str) -> dict:
        """Get ratings from OMDB by IMDB ID."""
        if not imdb_id or self._disabled:
            return {}

        try:
            client = await self._get_client()
            response = await client.get(
                self.BASE_URL,
                params={"i": imdb_id, "apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if data.get("Response") == "False":
                error = data.get("Error", "Unknown error")
                if "limit" in error.lower():
                    self._disabled = True
                return {}

            return self._parse_ratings(data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                self._disabled = True
            return {}
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
