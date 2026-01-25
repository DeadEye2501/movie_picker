from typing import Optional
import httpx


class MDBListAPI:
    """Wrapper for MDBList API to get ratings from multiple sources."""

    BASE_URL = "https://api.mdblist.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.Client(timeout=10.0)
        self._disabled = False

    def get_ratings_by_tmdb_id(self, tmdb_id: int, is_tv: bool = False) -> dict:
        """Get ratings from MDBList by TMDB ID.

        Returns dict with keys: imdb, rotten_tomatoes, metacritic, trakt, letterboxd
        """
        if not tmdb_id or self._disabled:
            return {}

        media_type = "show" if is_tv else "movie"

        try:
            response = self._client.get(
                f"{self.BASE_URL}/tmdb/{media_type}/{tmdb_id}",
                params={"apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if not data or "ratings" not in data:
                return {}

            return self._parse_ratings(data["ratings"])

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                print(f"[MDBList] Auth error ({e.response.status_code}) - disabling for this session")
                self._disabled = True
            elif e.response.status_code == 429:
                print("[MDBList] Rate limit reached - disabling for this session")
                self._disabled = True
            else:
                print(f"[MDBList] HTTP error: {e.response.status_code}")
            return {}
        except Exception as e:
            print(f"[MDBList] Error: {e}")
            return {}

    def get_ratings_by_imdb_id(self, imdb_id: str) -> dict:
        """Get ratings from MDBList by IMDB ID.

        Returns dict with keys: imdb, rotten_tomatoes, metacritic, trakt, letterboxd
        """
        if not imdb_id or self._disabled:
            return {}

        try:
            response = self._client.get(
                f"{self.BASE_URL}/imdb/{imdb_id}",
                params={"apikey": self.api_key},
            )
            response.raise_for_status()
            data = response.json()

            if not data or "ratings" not in data:
                return {}

            return self._parse_ratings(data["ratings"])

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                print(f"[MDBList] Auth error ({e.response.status_code}) - disabling for this session")
                self._disabled = True
            elif e.response.status_code == 429:
                print("[MDBList] Rate limit reached - disabling for this session")
                self._disabled = True
            else:
                print(f"[MDBList] HTTP error: {e.response.status_code}")
            return {}
        except Exception as e:
            print(f"[MDBList] Error: {e}")
            return {}

    def _parse_ratings(self, ratings: list) -> dict:
        """Parse ratings array from MDBList response."""
        result = {}

        for rating in ratings:
            source = rating.get("source", "").lower()
            value = rating.get("value")
            score = rating.get("score")  # Normalized 0-100 score

            if value is None:
                continue

            if source == "imdb":
                result["imdb"] = float(value)
            elif source == "tomatoes":
                # Rotten Tomatoes critic score (percentage)
                result["rotten_tomatoes"] = int(value)
            elif source == "metacritic":
                result["metacritic"] = int(value)
            elif source == "trakt":
                result["trakt"] = float(value)
            elif source == "letterboxd":
                result["letterboxd"] = float(value)
            elif source == "audience":
                # RT audience score
                result["rt_audience"] = int(value)

        return result
