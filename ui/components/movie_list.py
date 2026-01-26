from typing import Callable, Optional
import flet as ft

from database.models import Movie, UserRating
from ui.theme import COLORS
from .movie_card import MovieCard


class MovieList(ft.Container):
    """Paginated list of movie cards."""

    ITEMS_PER_PAGE = 10

    def __init__(
        self,
        on_rating_change: Optional[Callable[[Movie, int], None]] = None,
        on_review_click: Optional[Callable[[Movie], None]] = None,
        on_similar_click: Optional[Callable[[Movie], None]] = None,
        on_rating_delete: Optional[Callable[[Movie], None]] = None,
        on_wishlist_toggle: Optional[Callable[[Movie, bool], None]] = None,
        on_person_click: Optional[Callable[[str, str], None]] = None,
    ):
        self.movies: list[Movie] = []
        self.ratings: dict[int, UserRating] = {}
        self.wishlist_ids: set[int] = set()
        self.current_page = 0
        self.on_rating_change = on_rating_change
        self.on_review_click = on_review_click
        self.on_similar_click = on_similar_click
        self.on_rating_delete = on_rating_delete
        self.on_wishlist_toggle = on_wishlist_toggle
        self.on_person_click = on_person_click
        self.message: Optional[str] = None
        self.is_loading = False
        self.ratings_loading = False  # True when external ratings are being fetched

        self.movies_column = ft.Column(
            spacing=8,
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )
        self.dots_row = ft.Row(
            alignment=ft.MainAxisAlignment.CENTER,
            spacing=8,
        )
        self.dots_container = ft.Container(
            content=self.dots_row,
            padding=ft.padding.only(top=4, bottom=2),
            visible=False,
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

        super().__init__(
            content=ft.Column(
                controls=[
                    self.loading_indicator,
                    self.message_text,
                    self.movies_column,
                    self.dots_container,
                ],
                expand=True,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            expand=True,
        )

    def _build_dots(self, total_pages: int):
        """Build pagination dots."""
        self.dots_row.controls.clear()

        for i in range(total_pages):
            is_current = i == self.current_page
            dot = ft.Container(
                width=8,
                height=8,
                border_radius=4,
                bgcolor="#FFFFFF" if is_current else "#4DFFFFFF",
                on_click=lambda e, page=i: self._go_to_page(page),
            )
            self.dots_row.controls.append(dot)

    def show_loading(self):
        """Show loading indicator."""
        self.is_loading = True
        self.loading_indicator.visible = True
        self.message_text.visible = False
        self.movies_column.visible = False
        self.movies_column.controls.clear()
        self.dots_container.visible = False
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
        self.current_page = 0
        self.message = None
        self._refresh()

    def set_message(self, message: str):
        """Show a message instead of movies."""
        self.is_loading = False
        self.message = message
        self.movies = []
        self._refresh()

    def update_rating(self, movie_id: int, rating: UserRating):
        """Update rating for a specific movie (without full refresh)."""
        self.ratings[movie_id] = rating
        self._update_single_card(movie_id)

    def remove_rating(self, movie_id: int, remove_from_list: bool = False):
        """Remove rating for a specific movie.

        Args:
            movie_id: ID of the movie
            remove_from_list: If True, also remove movie from the list (for ratings view)
        """
        if movie_id in self.ratings:
            del self.ratings[movie_id]

        if remove_from_list:
            self.movies = [m for m in self.movies if m.id != movie_id]
            # Adjust current page if needed
            total_pages = max(1, (len(self.movies) + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
            if self.current_page >= total_pages:
                self.current_page = max(0, total_pages - 1)
            self._refresh()  # Need full refresh when removing from list
        else:
            self._update_single_card(movie_id)  # Just update the one card

    def _refresh(self):
        """Refresh the displayed content."""
        self.loading_indicator.visible = False
        self.movies_column.visible = True
        self.movies_column.controls.clear()

        if self.message:
            self.message_text.value = self.message
            self.message_text.visible = True
            self.dots_container.visible = False
        else:
            self.message_text.value = ""
            self.message_text.visible = False

            total_pages = max(1, (len(self.movies) + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
            self.dots_container.visible = total_pages > 1

            if total_pages > 1:
                self._build_dots(total_pages)

            start_idx = self.current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_movies = self.movies[start_idx:end_idx]

            for movie in page_movies:
                rating_obj = self.ratings.get(movie.id)
                user_rating = rating_obj.rating if rating_obj else None
                user_review = rating_obj.review if rating_obj else None
                in_wishlist = movie.id in self.wishlist_ids
                # Show loading indicator only for movies missing external ratings
                movie_ratings_loading = self.ratings_loading and (
                    movie.imdb_rating is None or
                    movie.kp_rating is None or
                    movie.rotten_tomatoes is None or
                    movie.metacritic is None
                )

                card = MovieCard(
                    movie=movie,
                    user_rating=user_rating,
                    user_review=user_review,
                    in_wishlist=in_wishlist,
                    ratings_loading=movie_ratings_loading,
                    on_rating_change=self.on_rating_change,
                    on_review_click=self.on_review_click,
                    on_similar_click=self.on_similar_click,
                    on_rating_delete=self.on_rating_delete,
                    on_wishlist_toggle=self.on_wishlist_toggle,
                    on_person_click=self.on_person_click,
                )
                self.movies_column.controls.append(card)

        self.update()

    def _update_single_card(self, movie_id: int):
        """Update a single card without rebuilding everything."""
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_movies = self.movies[start_idx:end_idx]

        for i, movie in enumerate(page_movies):
            if movie.id == movie_id and i < len(self.movies_column.controls):
                card = self.movies_column.controls[i]
                if isinstance(card, MovieCard):
                    rating_obj = self.ratings.get(movie_id)
                    card.user_rating = rating_obj.rating if rating_obj else None
                    card.user_review = rating_obj.review if rating_obj else None
                    card.in_wishlist = movie_id in self.wishlist_ids
                    card.content = card._build_content()
                    card.update()
                break

    def _go_to_page(self, page: int):
        """Navigate to a specific page."""
        total_pages = max(1, (len(self.movies) + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)

        if 0 <= page < total_pages:
            self.current_page = page
            self._refresh()

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
        total_pages = max(1, (len(self.movies) + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        if self.current_page >= total_pages:
            self.current_page = max(0, total_pages - 1)
        self._refresh()

    def update_movie_data(self, movie: Movie):
        """Update movie data (e.g., ratings) without full refresh."""
        # Update movie in list
        for i, m in enumerate(self.movies):
            if m.id == movie.id:
                self.movies[i] = movie
                break

        # Find and update only the specific card on current page
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_movies = self.movies[start_idx:end_idx]

        for i, m in enumerate(page_movies):
            if m.id == movie.id and i < len(self.movies_column.controls):
                card = self.movies_column.controls[i]
                if isinstance(card, MovieCard):
                    # Update card's movie data and rebuild its content
                    card.movie = movie
                    # Check if this movie still needs loading indicator
                    card.ratings_loading = self.ratings_loading and (
                        movie.imdb_rating is None or
                        movie.kp_rating is None or
                        movie.rotten_tomatoes is None or
                        movie.metacritic is None
                    )
                    card.content = card._build_content()
                    card.update()
                break

    def set_ratings_loading(self, loading: bool):
        """Set whether external ratings are being loaded."""
        if self.ratings_loading == loading:
            return  # No change
        self.ratings_loading = loading

        # Update only the loading state of visible cards without full rebuild
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_movies = self.movies[start_idx:end_idx]

        for i, movie in enumerate(page_movies):
            if i < len(self.movies_column.controls):
                card = self.movies_column.controls[i]
                if isinstance(card, MovieCard):
                    movie_ratings_loading = loading and (
                        movie.imdb_rating is None or
                        movie.kp_rating is None or
                        movie.rotten_tomatoes is None or
                        movie.metacritic is None
                    )
                    if card.ratings_loading != movie_ratings_loading:
                        card.ratings_loading = movie_ratings_loading
                        card.content = card._build_content()
                        card.update()
