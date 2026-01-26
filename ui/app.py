import os
import sys
import asyncio

import flet as ft

from api import TMDBAPI, OMDBAPI, KinopoiskAPI, MDBListAPI
from database import (
    init_db, close_db, get_session, save_user_rating, delete_user_rating,
    update_entity_ratings_for_movie, get_all_user_ratings_filtered, get_user_rating,
    is_in_wishlist, add_to_wishlist, remove_from_wishlist, get_wishlist, get_wishlist_movie_ids
)
from database.models import Movie, UserRating
from services import SearchService, RecommenderService
from ui.theme import COLORS, get_dark_theme
from ui.components import SearchBar, MovieList
from ui.components.rating_dialog import show_rating_dialog

# Global flag to signal background tasks to stop
_shutdown_event = asyncio.Event()


def is_shutting_down() -> bool:
    """Check if the application is shutting down."""
    return _shutdown_event.is_set()


class MoviePickerApp:
    """Main application class."""

    # Sort states: (sort_key, icon, arrow_icon)
    SORT_STATES = [
        ("rating_desc", ft.Icons.STAR, ft.Icons.ARROW_DOWNWARD),
        ("rating_asc", ft.Icons.STAR, ft.Icons.ARROW_UPWARD),
        ("title_asc", ft.Icons.SORT_BY_ALPHA, ft.Icons.ARROW_DOWNWARD),
        ("title_desc", ft.Icons.SORT_BY_ALPHA, ft.Icons.ARROW_UPWARD),
    ]

    def __init__(self, tmdb_api_key: str, omdb_api_key: str = None, kp_api_key: str = None, mdblist_api_key: str = None, db_path: str = "movie_picker.db"):
        self.db_path = db_path
        self.page: ft.Page = None
        self.search_bar: SearchBar = None
        self.movie_list: MovieList = None
        self.is_ratings_mode = False
        self.is_wishlist_mode = False
        self.sort_state_index = 0

        self.tmdb_api = TMDBAPI(tmdb_api_key)
        self.mdblist_api = MDBListAPI(mdblist_api_key) if mdblist_api_key else None
        self.omdb_api = OMDBAPI(omdb_api_key) if omdb_api_key else None
        self.kp_api = KinopoiskAPI(kp_api_key) if kp_api_key else None
        self.recommender = RecommenderService(self.tmdb_api)
        self.search_service = SearchService(self.tmdb_api, self.omdb_api, self.kp_api, self.mdblist_api, self.recommender)

    async def build(self, page: ft.Page):
        """Build the application UI."""
        self.page = page
        page.title = "Movie Picker"
        page.theme = get_dark_theme()
        page.theme_mode = ft.ThemeMode.DARK
        page.bgcolor = COLORS["background"]
        page.padding = 20
        page.window.width = 900
        page.window.height = 700

        # Initialize database
        await init_db(self.db_path)

        # Handle window close gracefully
        async def on_window_event(e):
            if e.data == "close":
                _shutdown_event.set()  # Signal background tasks to stop
                # Close API clients
                await self.tmdb_api.close()
                if self.omdb_api:
                    await self.omdb_api.close()
                if self.kp_api:
                    await self.kp_api.close()
                if self.mdblist_api:
                    await self.mdblist_api.close()
                await self.search_service.close()
                await close_db()  # Close database connections
                os._exit(0)

        page.window.on_event = on_window_event

        self.search_bar = SearchBar(
            on_search=self._handle_search,
            on_my_ratings=self._handle_my_ratings,
            on_wishlist=self._handle_wishlist,
            on_magic=self._handle_magic,
            on_genre_change=self._handle_genre_change,
        )

        self.movie_list = MovieList(
            on_rating_change=self._handle_rating_change,
            on_review_click=self._handle_review_click,
            on_similar_click=self._handle_similar_click,
            on_rating_delete=self._handle_rating_delete,
            on_wishlist_toggle=self._handle_wishlist_toggle,
            on_person_click=self._handle_person_click,
        )

        page.add(
            ft.Column(
                controls=[
                    self.search_bar,
                    self.movie_list,
                ],
                expand=True,
                spacing=8,
            )
        )

    def _handle_search(self, query: str, genres: list[int] = None):
        """Handle search button click."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._show_loading()

        async def do_search():
            if is_shutting_down():
                return
            try:
                async with get_session() as session:
                    if is_shutting_down():
                        return
                    # Load movies quickly without external ratings
                    movies = await self.search_service.search_movies(session, query, genres=genres or [], skip_ratings=True)

                    if is_shutting_down():
                        return
                    if movies:
                        ratings = await self._get_ratings_for_movies(movies)
                        wishlist_ids = await get_wishlist_movie_ids(session)
                        # Show movies with loading indicators for ratings
                        self.movie_list.set_movies(movies, ratings, wishlist_ids, ratings_loading=True)

                        # Load missing ratings in background
                        movies_to_load = movies  # Capture for closure
                        async def load_ratings():
                            await self._load_ratings_background(movies_to_load)
                        self.page.run_task(load_ratings)
                    else:
                        self.movie_list.set_message("По вашему запросу ничего не найдено")
            except Exception as e:
                if not is_shutting_down():
                    self.movie_list.set_message(f"Ошибка при поиске: {str(e)}")

        self.page.run_task(do_search)

    def _handle_my_ratings(self):
        """Handle my ratings button click - toggle sort or enter ratings mode."""
        self._exit_wishlist_mode()
        if self.is_ratings_mode:
            # Cycle through sort states
            self.sort_state_index = (self.sort_state_index + 1) % len(self.SORT_STATES)
            self._update_sort_button()
            self.page.run_task(self._load_filtered_ratings)
        else:
            # Enter ratings mode
            self.is_ratings_mode = True
            self.sort_state_index = 0
            self._update_sort_button()
            self.page.run_task(self._load_filtered_ratings)

    def _handle_wishlist(self):
        """Handle wishlist button click."""
        self._exit_ratings_mode()
        if self.is_wishlist_mode:
            # Exit wishlist mode
            self._exit_wishlist_mode()
        else:
            # Enter wishlist mode
            self.is_wishlist_mode = True
            self.search_bar.set_wishlist_active(True)
            self.page.run_task(self._load_wishlist)

    def _handle_genre_change(self):
        """Handle genre filter change."""
        if self.is_ratings_mode:
            self.page.run_task(self._load_filtered_ratings)

    def _update_sort_button(self):
        """Update the sort button icon based on current state."""
        sort_key, main_icon, arrow_icon = self.SORT_STATES[self.sort_state_index]
        self.search_bar.set_ratings_button_icons(main_icon, arrow_icon)

    def _exit_ratings_mode(self):
        """Exit ratings mode and restore normal button."""
        if self.is_ratings_mode:
            self.is_ratings_mode = False
            self.sort_state_index = 0
            self.search_bar.reset_ratings_button()

    def _exit_wishlist_mode(self):
        """Exit wishlist mode and restore normal button."""
        if self.is_wishlist_mode:
            self.is_wishlist_mode = False
            self.search_bar.set_wishlist_active(False)

    async def _load_filtered_ratings(self):
        """Load user ratings with current sort and genre filter applied."""
        try:
            async with get_session() as session:
                sort_key = self.SORT_STATES[self.sort_state_index][0]
                genres = self.search_bar.get_selected_genre_names()

                user_ratings = await get_all_user_ratings_filtered(
                    session,
                    sort_by=sort_key,
                    genres=genres if genres else None,
                )

                if not user_ratings:
                    if genres:
                        self.movie_list.set_message("Нет фильмов с выбранными жанрами")
                    else:
                        self.movie_list.set_message("Вы ещё не оценили ни одного фильма")
                else:
                    movies = [ur.movie for ur in user_ratings]
                    ratings = {ur.movie_id: ur for ur in user_ratings}
                    wishlist_ids = await get_wishlist_movie_ids(session)
                    self.movie_list.set_movies(movies, ratings, wishlist_ids)
        except Exception as e:
            self.movie_list.set_message(f"Ошибка при загрузке оценок: {str(e)}")

        self.page.update()

    async def _load_wishlist(self):
        """Load wishlist movies."""
        try:
            async with get_session() as session:
                wishlist_items = await get_wishlist(session)

                if not wishlist_items:
                    self.movie_list.set_message("Список «Хочу посмотреть» пуст")
                else:
                    movies = [item.movie for item in wishlist_items]
                    ratings = await self._get_ratings_for_movies(movies)
                    wishlist_ids = {item.movie_id for item in wishlist_items}
                    self.movie_list.set_movies(movies, ratings, wishlist_ids)
        except Exception as e:
            self.movie_list.set_message(f"Ошибка при загрузке списка: {str(e)}")

        self.page.update()

    def _handle_magic(self):
        """Handle magic button click - find the best unwatched movie."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._show_loading()

        async def do_magic():
            if is_shutting_down():
                return
            try:
                async with get_session() as session:
                    if is_shutting_down():
                        return
                    movie = await self.search_service.find_magic_recommendation(session)

                    if is_shutting_down():
                        return
                    if movie:
                        ratings = await self._get_ratings_for_movies([movie])
                        wishlist_ids = await get_wishlist_movie_ids(session)
                        self.movie_list.set_movies([movie], ratings, wishlist_ids, ratings_loading=True)

                        # Load missing ratings in background
                        movie_to_load = movie  # Capture for closure
                        async def load_ratings():
                            await self._load_ratings_background([movie_to_load])
                        self.page.run_task(load_ratings)
                    else:
                        self.movie_list.set_message("Оцените несколько фильмов, чтобы получить рекомендации")
            except Exception as e:
                if not is_shutting_down():
                    self.movie_list.set_message(f"Ошибка: {str(e)}")

        self.page.run_task(do_magic)

    def _handle_rating_change(self, movie: Movie, rating: int):
        """Handle rating change for a movie."""
        # Optimistic UI update - instant feedback
        from database.models import UserRating
        fake_rating = UserRating(movie_id=movie.id, rating=rating)
        self.movie_list.update_rating(movie.id, fake_rating)
        self.movie_list.update_wishlist(movie.id, False)
        self._show_snackbar(f"Оценка {rating}/10 сохранена")
        self.page.update()

        # Save to DB in background
        async def do_save():
            try:
                async with get_session() as session:
                    await save_user_rating(session, movie.id, rating)
                # Update entity ratings in background
                async with get_session() as session:
                    await update_entity_ratings_for_movie(session, movie.id)
            except Exception as e:
                # Revert on error
                self.movie_list.update_rating(movie.id, None)
                self._show_snackbar(f"Ошибка: {str(e)}")
                self.page.update()

        self.page.run_task(do_save)

    def _handle_rating_delete(self, movie: Movie):
        """Handle rating deletion for a movie."""
        # Optimistic UI update
        self.movie_list.remove_rating(movie.id, remove_from_list=self.is_ratings_mode)
        self._show_snackbar("Оценка удалена")
        self.page.update()

        # Delete from DB in background
        async def do_delete():
            try:
                async with get_session() as session:
                    deleted = await delete_user_rating(session, movie.id)
                    if not deleted:
                        self._show_snackbar("Оценка не найдена")
                        self.page.update()
                        return
                # Update entity ratings in background
                async with get_session() as session:
                    await update_entity_ratings_for_movie(session, movie.id)
            except Exception as e:
                self._show_snackbar(f"Ошибка: {str(e)}")
                self.page.update()

        self.page.run_task(do_delete)

    def _handle_wishlist_toggle(self, movie: Movie, add: bool):
        """Handle wishlist toggle for a movie."""
        # Optimistic UI update
        if add:
            self.movie_list.update_wishlist(movie.id, True)
            self._show_snackbar("Добавлено в «Хочу посмотреть»")
        else:
            if self.is_wishlist_mode:
                self.movie_list.remove_from_wishlist_view(movie.id)
            else:
                self.movie_list.update_wishlist(movie.id, False)
            self._show_snackbar("Удалено из «Хочу посмотреть»")
        self.page.update()

        # Save to DB in background
        async def do_toggle():
            try:
                async with get_session() as session:
                    if add:
                        await add_to_wishlist(session, movie.id)
                    else:
                        await remove_from_wishlist(session, movie.id)
            except Exception as e:
                # Revert on error
                if add:
                    self.movie_list.update_wishlist(movie.id, False)
                else:
                    self.movie_list.update_wishlist(movie.id, True)
                self._show_snackbar(f"Ошибка: {str(e)}")
                self.page.update()

        self.page.run_task(do_toggle)

    def _handle_person_click(self, name: str, person_type: str):
        """Handle click on director or actor name - search for their movies."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._show_loading()

        async def do_search():
            if is_shutting_down():
                return
            try:
                async with get_session() as session:
                    if is_shutting_down():
                        return
                    movies = await self.search_service.search_movies(session, name, skip_ratings=True)

                    if is_shutting_down():
                        return
                    if movies:
                        ratings = await self._get_ratings_for_movies(movies)
                        wishlist_ids = await get_wishlist_movie_ids(session)
                        self.movie_list.set_movies(movies, ratings, wishlist_ids, ratings_loading=True)

                        # Load missing ratings in background
                        movies_to_load = movies  # Capture for closure
                        async def load_ratings():
                            await self._load_ratings_background(movies_to_load)
                        self.page.run_task(load_ratings)
                    else:
                        self.movie_list.set_message(f"Фильмы с {name} не найдены")
            except Exception as e:
                if not is_shutting_down():
                    self.movie_list.set_message(f"Ошибка при поиске: {str(e)}")

        self.page.run_task(do_search)

    def _handle_review_click(self, movie: Movie):
        """Handle review button click."""
        async def do_show():
            try:
                async with get_session() as session:
                    user_rating = await get_user_rating(session, movie.id)
                    current_review = user_rating.review if user_rating else None

                    show_rating_dialog(
                        page=self.page,
                        movie=movie,
                        current_review=current_review,
                        on_save=self._handle_review_save,
                    )
            except Exception as e:
                self._show_snackbar(f"Ошибка: {str(e)}")

        self.page.run_task(do_show)

    def _handle_similar_click(self, movie: Movie):
        """Handle find similar button click."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._show_loading()

        async def do_similar():
            if is_shutting_down():
                return
            try:
                async with get_session() as session:
                    if is_shutting_down():
                        return
                    movies = await self.search_service.find_similar_movies(session, movie)

                    if is_shutting_down():
                        return
                    if movies:
                        ratings = await self._get_ratings_for_movies(movies)
                        wishlist_ids = await get_wishlist_movie_ids(session)
                        self.movie_list.set_movies(movies, ratings, wishlist_ids, ratings_loading=True)

                        # Load missing ratings in background
                        movies_to_load = movies  # Capture for closure
                        async def load_ratings():
                            await self._load_ratings_background(movies_to_load)
                        self.page.run_task(load_ratings)
                    else:
                        self.movie_list.set_message("Похожие фильмы не найдены")
            except Exception as e:
                if not is_shutting_down():
                    self.movie_list.set_message(f"Ошибка: {str(e)}")

        self.page.run_task(do_similar)

    def _handle_review_save(self, movie: Movie, review: str):
        """Handle review save."""
        async def do_save():
            try:
                async with get_session() as session:
                    existing_rating = await get_user_rating(session, movie.id)
                    rating = existing_rating.rating if existing_rating else 5

                    user_rating = await save_user_rating(session, movie.id, rating, review)

                    self.movie_list.update_rating(movie.id, user_rating)
                    self._show_snackbar("Рецензия сохранена")
            except Exception as e:
                self._show_snackbar(f"Ошибка при сохранении рецензии: {str(e)}")

            self.page.update()

        self.page.run_task(do_save)

    async def _get_ratings_for_movies(self, movies: list[Movie]) -> dict[int, UserRating]:
        """Get user ratings for a list of movies."""
        ratings = {}
        async with get_session() as session:
            for movie in movies:
                user_rating = await get_user_rating(session, movie.id)
                if user_rating:
                    ratings[movie.id] = user_rating
        return ratings

    async def _load_ratings_background(self, movies: list):
        """Load missing ratings in background and update UI."""
        if is_shutting_down():
            return
        try:
            async with get_session() as session:
                def on_movie_updated(movie):
                    if is_shutting_down():
                        return
                    # Update UI when a movie's ratings are loaded
                    self.movie_list.update_movie_data(movie)

                await self.search_service.fetch_missing_ratings(session, movies, on_movie_updated)
        except Exception:
            pass  # Silently ignore rating fetch errors
        finally:
            if not is_shutting_down():
                # Turn off loading indicators when done
                self.movie_list.set_ratings_loading(False)

    def _show_loading(self):
        """Show loading indicator."""
        self.movie_list.show_loading()
        self.page.update()

    def _show_snackbar(self, message: str):
        """Show a snackbar notification."""
        def on_dismiss(e):
            if snackbar in self.page.overlay:
                self.page.overlay.remove(snackbar)

        snackbar = ft.SnackBar(
            content=ft.Text(message, color=COLORS["text_primary"]),
            bgcolor=COLORS["surface_variant"],
            on_dismiss=on_dismiss,
        )
        self.page.overlay.append(snackbar)
        snackbar.open = True
        self.page.update()
