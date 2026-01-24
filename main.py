import os
import sys
from pathlib import Path

import flet as ft
from dotenv import load_dotenv

from ui import MoviePickerApp


def get_app_dir() -> Path:
    """Get the directory where the app/exe is located."""
    if getattr(sys, 'frozen', False):
        # Running as compiled exe
        return Path(sys.executable).parent
    else:
        # Running as script
        return Path(__file__).parent


def load_config() -> dict:
    """Load configuration from .env file."""
    app_dir = get_app_dir()
    env_path = app_dir / ".env"
    load_dotenv(env_path)

    tmdb_key = os.getenv("TMDB_API_KEY")
    omdb_key = os.getenv("OMDB_API_KEY")

    if not tmdb_key:
        print("Error: TMDB_API_KEY not found in .env")
        print("Please create .env file with your API keys:")
        print("TMDB_API_KEY=your_key_here")
        print("OMDB_API_KEY=your_key_here  # optional")
        print("\nGet your TMDB key at: https://www.themoviedb.org/settings/api")
        sys.exit(1)

    return {
        "tmdb_api_key": tmdb_key,
        "omdb_api_key": omdb_key,
    }


def main(page: ft.Page):
    """Main entry point."""
    config = load_config()
    app_dir = get_app_dir()
    db_path = str(app_dir / "movie_picker.db")

    app = MoviePickerApp(
        tmdb_api_key=config["tmdb_api_key"],
        omdb_api_key=config.get("omdb_api_key"),
        db_path=db_path,
    )
    app.build(page)


if __name__ == "__main__":
    ft.app(main)
