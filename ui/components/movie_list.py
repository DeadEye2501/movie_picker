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
    ):
        self.movies: list[Movie] = []
        self.ratings: dict[int, UserRating] = {}
        self.current_page = 0
        self.on_rating_change = on_rating_change
        self.on_review_click = on_review_click
        self.on_similar_click = on_similar_click
        self.on_rating_delete = on_rating_delete
        self.message: Optional[str] = None
        self.is_loading = False

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

    def set_movies(self, movies: list[Movie], ratings: Optional[dict[int, UserRating]] = None):
        """Set the list of movies to display."""
        self.is_loading = False
        self.movies = movies
        self.ratings = ratings or {}
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
        """Update rating for a specific movie."""
        self.ratings[movie_id] = rating
        self._refresh()

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

        self._refresh()

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

                card = MovieCard(
                    movie=movie,
                    user_rating=user_rating,
                    user_review=user_review,
                    on_rating_change=self.on_rating_change,
                    on_review_click=self.on_review_click,
                    on_similar_click=self.on_similar_click,
                    on_rating_delete=self.on_rating_delete,
                )
                self.movies_column.controls.append(card)

        self.update()

    def _go_to_page(self, page: int):
        """Navigate to a specific page."""
        total_pages = max(1, (len(self.movies) + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)

        if 0 <= page < total_pages:
            self.current_page = page
            self._refresh()
