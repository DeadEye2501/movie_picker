import threading
from typing import Callable, Optional
import flet as ft

from database.models import Movie, UserRating
from ui.theme import COLORS
from .movie_card import MovieCard


class MovieList(ft.Container):
    """List of movie cards with infinite scroll."""

    ITEMS_PER_PAGE = 20  # Items to load per batch for infinite scroll

    def __init__(
        self,
        on_rating_change: Optional[Callable[[Movie, int], None]] = None,
        on_review_click: Optional[Callable[[Movie], None]] = None,
        on_similar_click: Optional[Callable[[Movie], None]] = None,
        on_rating_delete: Optional[Callable[[Movie], None]] = None,
        on_wishlist_toggle: Optional[Callable[[Movie, bool], None]] = None,
        on_person_click: Optional[Callable[[str, str], None]] = None,
        on_tags_click: Optional[Callable[[Movie], None]] = None,
        on_fetch_more: Optional[Callable[[], None]] = None,
    ):
        self.movies: list[Movie] = []
        self.ratings: dict[int, UserRating] = {}
        self.wishlist_ids: set[int] = set()
        self.movie_tags: dict[int, list[str]] = {}  # movie_id -> list of tag names
        self.loaded_count = 0  # How many items are currently rendered
        self._scroll_sem = threading.Semaphore()  # Prevent concurrent loads (official Flet pattern)
        self.on_rating_change = on_rating_change
        self.on_review_click = on_review_click
        self.on_similar_click = on_similar_click
        self.on_rating_delete = on_rating_delete
        self.on_wishlist_toggle = on_wishlist_toggle
        self.on_person_click = on_person_click
        self.on_tags_click = on_tags_click
        self.on_fetch_more = on_fetch_more  # Called when all local results shown, needs more from API
        self.message: Optional[str] = None
        self._custom_content: Optional[ft.Control] = None
        self.is_loading = False
        self.ratings_loading = False  # True when external ratings are being fetched
        self.all_collapsed = False  # Global collapse state
        self._fetching_more = False  # True when API fetch is in progress

        self.movies_column = ft.ListView(
            spacing=0,
            expand=True,
            auto_scroll=False,
        )
        self.message_text = ft.Text(
            "",
            size=16,
            color=COLORS["text_secondary"],
            text_align=ft.TextAlign.CENTER,
        )
        self.loading_indicator = ft.Container(
            content=ft.ProgressRing(width=40, height=40, stroke_width=3, color=COLORS["primary"]),
            alignment=ft.Alignment(0, -0.08),
            expand=True,
            visible=False,
        )
        self.custom_content_container = ft.Container(
            expand=True,
            visible=False,
        )

        super().__init__(
            content=ft.Column(
                controls=[
                    self.loading_indicator,
                    self.message_text,
                    self.custom_content_container,
                    self.movies_column,
                ],
                expand=True,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            expand=True,
        )

    def _build_load_more_row(self) -> ft.Container:
        """Build a row with 'Load more' button."""
        return ft.Container(
            content=ft.Row(
                controls=[
                    ft.TextButton(
                        "Загрузить ещё",
                        on_click=lambda e: self._load_more_click(),
                        style=ft.ButtonStyle(color=COLORS["primary"]),
                    ),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            padding=ft.padding.symmetric(vertical=12),
            data="load_more_row",
        )

    def _has_more(self) -> bool:
        return self.loaded_count < len(self.movies) or self.on_fetch_more is not None

    def _remove_load_more_row(self):
        to_remove = [
            c for c in self.movies_column.controls
            if isinstance(c, ft.Container) and getattr(c, 'data', None) == "load_more_row"
        ]
        for c in to_remove:
            self.movies_column.controls.remove(c)

    def _append_load_more_if_needed(self):
        if self._has_more():
            self.movies_column.controls.append(self._build_load_more_row())

    def toggle_all_collapsed(self):
        """Toggle collapse state for all cards."""
        self.all_collapsed = not self.all_collapsed

        for control in self.movies_column.controls:
            if isinstance(control, MovieCard):
                control.collapsed = self.all_collapsed
                control._apply_collapse_state()
                control.update()

    def _on_scroll(self, e: ft.OnScrollEvent):
        """Handle scroll event — official Flet pattern with Semaphore."""
        if e.pixels >= e.max_scroll_extent - 200:
            if self._scroll_sem.acquire(blocking=False):
                try:
                    if self._has_more():
                        self._remove_load_more_row()
                        self._do_load_batch()
                        self._append_load_more_if_needed()
                        self.movies_column.update()
                finally:
                    self._scroll_sem.release()

    def _load_more_click(self):
        """Handle 'Load more' button click."""
        if self._fetching_more:
            return
        if self._scroll_sem.acquire(blocking=False):
            try:
                if self.loaded_count < len(self.movies):
                    # Show more from already-loaded movies
                    self._remove_load_more_row()
                    self._do_load_batch()
                    self._append_load_more_if_needed()
                    self.movies_column.update()
                elif self.on_fetch_more is not None:
                    # Need more from API
                    self._fetching_more = True
                    self._remove_load_more_row()
                    self.movies_column.controls.append(self._build_fetching_row())
                    self.movies_column.update()
                    self.on_fetch_more()
            finally:
                self._scroll_sem.release()

    def _build_fetching_row(self) -> ft.Container:
        """Build a row with spinner shown while fetching more from API."""
        return ft.Container(
            content=ft.Row(
                controls=[
                    ft.ProgressRing(width=20, height=20, stroke_width=2, color=COLORS["primary"]),
                    ft.Text("Загрузка...", size=14, color=COLORS["text_secondary"]),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                spacing=8,
            ),
            padding=ft.padding.symmetric(vertical=12),
            data="load_more_row",
        )

    def append_movies(self, new_movies: list[Movie], ratings: dict[int, UserRating] = None,
                      wishlist_ids: set[int] = None):
        """Append more movies from API fetch (keeps existing cards)."""
        self._fetching_more = False
        if ratings:
            self.ratings.update(ratings)
        if wishlist_ids is not None:
            self.wishlist_ids.update(wishlist_ids)

        existing_ids = {m.id for m in self.movies}
        added = [m for m in new_movies if m.id not in existing_ids]
        self.movies.extend(added)

        # Remove fetching indicator
        self._remove_load_more_row()

        if added:
            # Show next batch of cards
            self._do_load_batch()

        self._append_load_more_if_needed()
        self.movies_column.update()

    def _do_load_batch(self):
        """Add next batch of cards to the column (no update call)."""
        end_idx = min(self.loaded_count + self.ITEMS_PER_PAGE, len(self.movies))
        for movie in self.movies[self.loaded_count:end_idx]:
            card = self._create_card(movie)
            self.movies_column.controls.append(card)
        self.loaded_count = end_idx

    def _create_card(self, movie: Movie) -> MovieCard:
        """Create a MovieCard for the given movie."""
        rating_obj = self.ratings.get(movie.id)
        user_rating = rating_obj.rating if rating_obj else None
        user_review = rating_obj.review if rating_obj else None
        in_wishlist = movie.id in self.wishlist_ids
        tags = self.movie_tags.get(movie.id, [])
        movie_ratings_loading = self.ratings_loading and (
            movie.imdb_rating is None or
            movie.kp_rating is None or
            movie.rotten_tomatoes is None or
            movie.metacritic is None
        )

        return MovieCard(
            movie=movie,
            user_rating=user_rating,
            user_review=user_review,
            user_tags=tags,
            in_wishlist=in_wishlist,
            ratings_loading=movie_ratings_loading,
            collapsed=self.all_collapsed,
            on_rating_change=self.on_rating_change,
            on_review_click=self.on_review_click,
            on_similar_click=self.on_similar_click,
            on_rating_delete=self.on_rating_delete,
            on_wishlist_toggle=self.on_wishlist_toggle,
            on_person_click=self.on_person_click,
            on_tags_click=self.on_tags_click,
        )

    def show_loading(self):
        """Show loading indicator."""
        self.is_loading = True
        self.loading_indicator.visible = True
        self.message_text.visible = False
        self.movies_column.visible = False
        self.custom_content_container.visible = False
        self._custom_content = None
        self.movies_column.controls.clear()
        self.loaded_count = 0
        self.update()

    def set_movies(
        self,
        movies: list[Movie],
        ratings: Optional[dict[int, UserRating]] = None,
        wishlist_ids: Optional[set[int]] = None,
        ratings_loading: bool = False
    ):
        """Set the list of movies to display."""
        self.is_loading = False
        self.movies = movies
        self.ratings = ratings or {}
        self.wishlist_ids = wishlist_ids or set()
        self.ratings_loading = ratings_loading
        self.loaded_count = 0
        self.message = None
        self._custom_content = None
        self._refresh()

    def set_message(self, message: str):
        """Show a message instead of movies."""
        self.is_loading = False
        self.message = message
        self.movies = []
        self.loaded_count = 0
        self._custom_content = None
        self._refresh()

    def set_custom_content(self, control: ft.Control):
        """Show custom content instead of movies (e.g. stats)."""
        self.is_loading = False
        self.message = None
        self.movies = []
        self.loaded_count = 0
        self._custom_content = control
        self._refresh()

    def update_rating(self, movie_id: int, rating: UserRating):
        """Update rating for a specific movie (without full refresh)."""
        self.ratings[movie_id] = rating
        self._update_single_card(movie_id)

    def remove_rating(self, movie_id: int, remove_from_list: bool = False):
        """Remove rating for a specific movie."""
        if movie_id in self.ratings:
            del self.ratings[movie_id]

        if remove_from_list:
            self.movies = [m for m in self.movies if m.id != movie_id]
            to_remove = [c for c in self.movies_column.controls if isinstance(c, MovieCard) and c.movie.id == movie_id]
            for c in to_remove:
                self.movies_column.controls.remove(c)
            self.loaded_count = len([c for c in self.movies_column.controls if isinstance(c, MovieCard)])
            self.update()
        else:
            self._update_single_card(movie_id)

    def _refresh(self):
        """Refresh the displayed content."""
        self.loading_indicator.visible = False
        self.movies_column.controls.clear()
        self.loaded_count = 0

        if self._custom_content:
            self.message_text.visible = False
            self.movies_column.visible = False
            self.custom_content_container.content = self._custom_content
            self.custom_content_container.visible = True
        elif self.message:
            self.custom_content_container.visible = False
            self.movies_column.visible = True
            self.message_text.value = self.message
            self.message_text.visible = True
        else:
            self.custom_content_container.visible = False
            self.movies_column.visible = True
            self.message_text.value = ""
            self.message_text.visible = False

            # Load first batch
            end_idx = min(self.ITEMS_PER_PAGE, len(self.movies))
            for movie in self.movies[:end_idx]:
                card = self._create_card(movie)
                self.movies_column.controls.append(card)
            self.loaded_count = end_idx

            # Add load-more row if needed
            self._append_load_more_if_needed()

        self.update()

    def _update_single_card(self, movie_id: int):
        """Update a single card without rebuilding everything."""
        for card in self.movies_column.controls:
            if isinstance(card, MovieCard) and card.movie.id == movie_id:
                rating_obj = self.ratings.get(movie_id)
                card.user_rating = rating_obj.rating if rating_obj else None
                card.user_review = rating_obj.review if rating_obj else None
                card.in_wishlist = movie_id in self.wishlist_ids
                card.user_tags = self.movie_tags.get(movie_id, [])
                card.invalidate_view_cache()
                card._apply_collapse_state()
                card.update()
                break

    def update_movie_tags(self, movie_id: int, tags: list[str]):
        """Update tags for a movie."""
        self.movie_tags[movie_id] = tags
        self._update_single_card(movie_id)

    def update_wishlist(self, movie_id: int, in_wishlist: bool):
        """Update wishlist status for a movie (without full refresh)."""
        if in_wishlist:
            self.wishlist_ids.add(movie_id)
        else:
            self.wishlist_ids.discard(movie_id)
        self._update_single_card(movie_id)

    def remove_from_wishlist_view(self, movie_id: int):
        """Remove movie from wishlist view (when in wishlist mode)."""
        self.wishlist_ids.discard(movie_id)
        self.movies = [m for m in self.movies if m.id != movie_id]
        to_remove = [c for c in self.movies_column.controls if isinstance(c, MovieCard) and c.movie.id == movie_id]
        for c in to_remove:
            self.movies_column.controls.remove(c)
        self.loaded_count = len([c for c in self.movies_column.controls if isinstance(c, MovieCard)])
        self.update()

    def update_movie_data(self, movie: Movie):
        """Update movie data (e.g., ratings) without full refresh."""
        for i, m in enumerate(self.movies):
            if m.id == movie.id:
                self.movies[i] = movie
                break

        for card in self.movies_column.controls:
            if isinstance(card, MovieCard) and card.movie.id == movie.id:
                card.movie = movie
                card.ratings_loading = self.ratings_loading and (
                    movie.imdb_rating is None or
                    movie.kp_rating is None or
                    movie.rotten_tomatoes is None or
                    movie.metacritic is None
                )
                card.invalidate_view_cache()
                card._apply_collapse_state()
                card.update()
                break

    def set_ratings_loading(self, loading: bool):
        """Set whether external ratings are being loaded."""
        if self.ratings_loading == loading:
            return
        self.ratings_loading = loading

        for card in self.movies_column.controls:
            if isinstance(card, MovieCard):
                movie = card.movie
                movie_ratings_loading = loading and (
                    movie.imdb_rating is None or
                    movie.kp_rating is None or
                    movie.rotten_tomatoes is None or
                    movie.metacritic is None
                )
                if card.ratings_loading != movie_ratings_loading:
                    card.ratings_loading = movie_ratings_loading
                    card.invalidate_view_cache()
                    card._apply_collapse_state()
                    card.update()
