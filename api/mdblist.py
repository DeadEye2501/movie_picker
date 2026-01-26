from typing import Optional
import httpx


class MDBListAPI:
    """Async wrapper for MDBList API to get ratings from multiple sources."""

    BASE_URL = "https://api.mdblist.com"

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

    async def get_ratings_by_tmdb_id(self, tmdb_id: int, is_tv: bool = False) -> dict:
        """Get ratings from MDBList by TMDB ID."""
        if not tmdb_id or self._disabled:
            return {}

        media_type = "show" if is_tv else "movie"

        try:
            client = await self._get_client()
            response = await client.get(
                f"{self.BASE_URL}/tmdb/{media_type}/{tmdb_id}",
                params={"apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if not data or "ratings" not in data:
                return {}

            return self._parse_ratings(data["ratings"])

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 429):
                self._disabled = True
            return {}
        except Exception:
            return {}

    async def get_ratings_by_imdb_id(self, imdb_id: str) -> dict:
        """Get ratings from MDBList by IMDB ID."""
        if not imdb_id or self._disabled:
            return {}

        try:
            client = await self._get_client()
            response = await client.get(
                f"{self.BASE_URL}/imdb/{imdb_id}",
                params={"apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if not data or "ratings" not in data:
                return {}

            return self._parse_ratings(data["ratings"])

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 429):
                self._disabled = True
            return {}
        except Exception:
            return {}

    def _parse_ratings(self, ratings: list) -> dict:
        """Parse ratings array from MDBList response."""
        result = {}

        for rating in ratings:
            source = rating.get("source", "").lower()
            value = rating.get("value")

            if value is None:
                continue

            if source == "imdb":
                result["imdb"] = float(value)
            elif source == "tomatoes":
                result["rotten_tomatoes"] = int(value)
            elif source == "metacritic":
                result["metacritic"] = int(value)
            elif source == "trakt":
                result["trakt"] = float(value)
            elif source == "letterboxd":
                result["letterboxd"] = float(value)
            elif source == "audience":
                result["rt_audience"] = int(value)

        return result
