import flet as ft

from api import TMDBAPI, OMDBAPI
from database import init_db, get_session, save_user_rating, delete_user_rating, get_all_user_ratings_filtered, get_user_rating
from database.models import Movie, UserRating
from services import SearchService, RecommenderService
from ui.theme import COLORS, get_dark_theme
from ui.components import SearchBar, MovieList
from ui.components.rating_dialog import show_rating_dialog


class MoviePickerApp:
    """Main application class."""

    # Sort states: (sort_key, icon, arrow_icon)
    SORT_STATES = [
        ("rating_desc", ft.Icons.STAR, ft.Icons.ARROW_DOWNWARD),
        ("rating_asc", ft.Icons.STAR, ft.Icons.ARROW_UPWARD),
        ("title_asc", ft.Icons.SORT_BY_ALPHA, ft.Icons.ARROW_DOWNWARD),
        ("title_desc", ft.Icons.SORT_BY_ALPHA, ft.Icons.ARROW_UPWARD),
    ]

    def __init__(self, tmdb_api_key: str, omdb_api_key: str = None, db_path: str = "movie_picker.db"):
        self.db_path = db_path
        self.page: ft.Page = None
        self.search_bar: SearchBar = None
        self.movie_list: MovieList = None
        self.is_ratings_mode = False
        self.sort_state_index = 0

        init_db(db_path)

        self.tmdb_api = TMDBAPI(tmdb_api_key)
        self.omdb_api = OMDBAPI(omdb_api_key) if omdb_api_key else None
        self.recommender = RecommenderService(self.tmdb_api)
        self.search_service = SearchService(self.tmdb_api, self.omdb_api, self.recommender)

    def build(self, page: ft.Page):
        """Build the application UI."""
        self.page = page
        page.title = "Movie Picker"
        page.theme = get_dark_theme()
        page.theme_mode = ft.ThemeMode.DARK
        page.bgcolor = COLORS["background"]
        page.padding = 20
        page.window.width = 900
        page.window.height = 700

        self.search_bar = SearchBar(
            on_search=self._handle_search,
            on_my_ratings=self._handle_my_ratings,
            on_magic=self._handle_magic,
            on_genre_change=self._handle_genre_change,
        )

        self.movie_list = MovieList(
            on_rating_change=self._handle_rating_change,
            on_review_click=self._handle_review_click,
            on_similar_click=self._handle_similar_click,
            on_rating_delete=self._handle_rating_delete,
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
        self._show_loading()

        def do_search():
            session = None
            try:
                session = get_session()
                movies = self.search_service.search_movies(session, query, genres=genres or [])

                if movies:
                    ratings = self._get_ratings_for_movies(movies)
                    self.movie_list.set_movies(movies, ratings)
                else:
                    self.movie_list.set_message("По вашему запросу ничего не найдено")

            except Exception as e:
                self.movie_list.set_message(f"Ошибка при поиске: {str(e)}")
            finally:
                if session:
                    session.close()

        self.page.run_thread(do_search)

    def _handle_my_ratings(self):
        """Handle my ratings button click - toggle sort or enter ratings mode."""
        if self.is_ratings_mode:
            # Cycle through sort states
            self.sort_state_index = (self.sort_state_index + 1) % len(self.SORT_STATES)
            self._update_sort_button()
            self._load_filtered_ratings()
        else:
            # Enter ratings mode
            self.is_ratings_mode = True
            self.sort_state_index = 0
            self._update_sort_button()
            self._load_filtered_ratings()

    def _handle_genre_change(self):
        """Handle genre filter change."""
        if self.is_ratings_mode:
            self._load_filtered_ratings()

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

    def _load_filtered_ratings(self):
        """Load user ratings with current sort and genre filter applied."""
        session = None
        try:
            session = get_session()

            sort_key = self.SORT_STATES[self.sort_state_index][0]
            genres = self.search_bar.get_selected_genre_names()

            user_ratings = get_all_user_ratings_filtered(
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
                self.movie_list.set_movies(movies, ratings)

        except Exception as e:
            self.movie_list.set_message(f"Ошибка при загрузке оценок: {str(e)}")
        finally:
            if session:
                session.close()

        self.page.update()

    def _handle_magic(self):
        """Handle magic button click - find the best unwatched movie."""
        self._exit_ratings_mode()
        self._show_loading()

        def do_magic():
            session = None
            try:
                session = get_session()
                movie = self.search_service.find_magic_recommendation(session)

                if movie:
                    ratings = self._get_ratings_for_movies([movie])
                    self.movie_list.set_movies([movie], ratings)
                else:
                    self.movie_list.set_message("Оцените несколько фильмов, чтобы получить рекомендации")

            except Exception as e:
                self.movie_list.set_message(f"Ошибка: {str(e)}")
            finally:
                if session:
                    session.close()

        self.page.run_thread(do_magic)

    def _handle_rating_change(self, movie: Movie, rating: int):
        """Handle rating change for a movie."""
        session = None
        try:
            session = get_session()
            user_rating = save_user_rating(session, movie.id, rating)

            self.movie_list.update_rating(movie.id, user_rating)
            self._show_snackbar(f"Оценка {rating}/10 сохранена")

        except Exception as e:
            self._show_snackbar(f"Ошибка при сохранении оценки: {str(e)}")
        finally:
            if session:
                session.close()

        self.page.update()

    def _handle_rating_delete(self, movie: Movie):
        """Handle rating deletion for a movie."""
        session = None
        try:
            session = get_session()
            deleted = delete_user_rating(session, movie.id)

            if deleted:
                # In ratings mode, remove movie from list; otherwise just clear the rating display
                self.movie_list.remove_rating(movie.id, remove_from_list=self.is_ratings_mode)
                self._show_snackbar("Оценка удалена")
            else:
                self._show_snackbar("Оценка не найдена")

        except Exception as e:
            self._show_snackbar(f"Ошибка при удалении оценки: {str(e)}")
        finally:
            if session:
                session.close()

        self.page.update()

    def _handle_review_click(self, movie: Movie):
        """Handle review button click."""
        session = None
        try:
            session = get_session()
            user_rating = get_user_rating(session, movie.id)
            current_review = user_rating.review if user_rating else None

            show_rating_dialog(
                page=self.page,
                movie=movie,
                current_review=current_review,
                on_save=self._handle_review_save,
            )

        except Exception as e:
            self._show_snackbar(f"Ошибка: {str(e)}")
        finally:
            if session:
                session.close()

    def _handle_similar_click(self, movie: Movie):
        """Handle find similar button click."""
        self._exit_ratings_mode()
        self._show_loading()

        def do_similar():
            session = None
            try:
                session = get_session()
                movies = self.search_service.find_similar_movies(session, movie)

                if movies:
                    ratings = self._get_ratings_for_movies(movies)
                    self.movie_list.set_movies(movies, ratings)
                else:
                    self.movie_list.set_message("Похожие фильмы не найдены")

            except Exception as e:
                self.movie_list.set_message(f"Ошибка: {str(e)}")
            finally:
                if session:
                    session.close()

        self.page.run_thread(do_similar)

    def _handle_review_save(self, movie: Movie, review: str):
        """Handle review save."""
        session = None
        try:
            session = get_session()
            existing_rating = get_user_rating(session, movie.id)
            rating = existing_rating.rating if existing_rating else 5

            user_rating = save_user_rating(session, movie.id, rating, review)

            self.movie_list.update_rating(movie.id, user_rating)
            self._show_snackbar("Рецензия сохранена")

        except Exception as e:
            self._show_snackbar(f"Ошибка при сохранении рецензии: {str(e)}")
        finally:
            if session:
                session.close()

        self.page.update()

    def _get_ratings_for_movies(self, movies: list[Movie]) -> dict[int, UserRating]:
        """Get user ratings for a list of movies."""
        session = None
        ratings = {}
        try:
            session = get_session()

            for movie in movies:
                user_rating = get_user_rating(session, movie.id)
                if user_rating:
                    ratings[movie.id] = user_rating
        finally:
            if session:
                session.close()
        return ratings

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
