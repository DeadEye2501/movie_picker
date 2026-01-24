from typing import Callable, Optional
import flet as ft

from database.models import Movie
from ui.theme import COLORS


class MovieCard(ft.Container):
    """A card component displaying movie information."""

    def __init__(
        self,
        movie: Movie,
        user_rating: Optional[int] = None,
        user_review: Optional[str] = None,
        on_rating_change: Optional[Callable[[Movie, int], None]] = None,
        on_review_click: Optional[Callable[[Movie], None]] = None,
        on_similar_click: Optional[Callable[[Movie], None]] = None,
    ):
        self.movie = movie
        self.user_rating = user_rating
        self.user_review = user_review
        self.on_rating_change = on_rating_change
        self.on_review_click = on_review_click
        self.on_similar_click = on_similar_click
        self.description_expanded = False

        super().__init__(
            content=self._build_content(),
            bgcolor=COLORS["surface"],
            border_radius=12,
            padding=16,
            margin=ft.margin.only(bottom=12),
        )

    def _build_content(self) -> ft.Control:
        return ft.Row(
            controls=[
                self._build_poster(),
                ft.Container(width=16),
                ft.Column(
                    controls=[
                        self._build_title_row(),
                        self._build_info_text(self.movie.genres, "Жанры: "),
                        self._build_info_text(self.movie.director, "Режиссёр: "),
                        self._build_info_text(self.movie.actors, "Актёры: "),
                        self._build_description(),
                        ft.Divider(height=1, color=COLORS["divider"]),
                        self._build_ratings_row(),
                        self._build_actions_row(),
                    ],
                    spacing=6,
                    expand=True,
                ),
            ],
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.START,
        )

    def _build_poster(self) -> ft.Control:
        if self.movie.poster_url:
            return ft.Container(
                content=ft.Image(
                    src=self.movie.poster_url,
                    width=120,
                    height=180,
                    fit="cover",
                    border_radius=8,
                ),
                width=120,
                height=180,
                border_radius=8,
                bgcolor=COLORS["surface_variant"],
            )
        else:
            return ft.Container(
                content=ft.Icon(ft.Icons.MOVIE, size=48, color=COLORS["text_secondary"]),
                width=120,
                height=180,
                bgcolor=COLORS["surface_variant"],
                border_radius=8,
                alignment=ft.Alignment(0, 0),
            )

    def _build_title_row(self) -> ft.Control:
        title_text = self.movie.title or "Без названия"
        year_text = f" ({self.movie.year})" if self.movie.year else ""

        return ft.Text(
            f"{title_text}{year_text}",
            size=18,
            weight=ft.FontWeight.BOLD,
            color=COLORS["text_primary"],
            max_lines=2,
            overflow=ft.TextOverflow.ELLIPSIS,
        )

    def _build_info_text(self, value: Optional[str], prefix: str = "") -> ft.Control:
        if not value:
            return ft.Container(height=0)

        return ft.Text(
            f"{prefix}{value}",
            size=13,
            color=COLORS["text_secondary"],
            max_lines=2,
            overflow=ft.TextOverflow.ELLIPSIS,
        )

    def _build_description(self) -> ft.Control:
        if not self.movie.description:
            return ft.Container(height=0)

        description = self.movie.description
        is_long = len(description) > 150

        if not is_long:
            return ft.Text(description, size=12, color=COLORS["text_secondary"])

        if self.description_expanded:
            # Full description with "свернуть"
            return ft.Text(
                spans=[
                    ft.TextSpan(description, style=ft.TextStyle(color=COLORS["text_secondary"])),
                    ft.TextSpan(
                        " свернуть",
                        style=ft.TextStyle(color=COLORS["primary"]),
                        on_click=self._toggle_description,
                    ),
                ],
                size=12,
            )
        else:
            # Truncated description with "ещё"
            return ft.Text(
                spans=[
                    ft.TextSpan(description[:150] + "... ", style=ft.TextStyle(color=COLORS["text_secondary"])),
                    ft.TextSpan(
                        "ещё",
                        style=ft.TextStyle(color=COLORS["primary"]),
                        on_click=self._toggle_description,
                    ),
                ],
                size=12,
            )

    def _toggle_description(self, e):
        self.description_expanded = not self.description_expanded
        self.content = self._build_content()
        self.update()

    def _build_ratings_row(self) -> ft.Control:
        """Build row with ratings from multiple sources."""
        ratings_parts = []

        # TMDB rating
        if self.movie.tmdb_rating:
            ratings_parts.append(ft.Text("TMDB ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.tmdb_rating:.1f}", size=12, color="#01D277", weight=ft.FontWeight.BOLD))

        # IMDB rating
        if self.movie.imdb_rating:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text("IMDB ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.imdb_rating:.1f}", size=12, color="#F5C518", weight=ft.FontWeight.BOLD))

        # Rotten Tomatoes
        if self.movie.rotten_tomatoes:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            rt_color = "#FA320A" if self.movie.rotten_tomatoes < 60 else "#0AC855"
            ratings_parts.append(ft.Text("RT ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.rotten_tomatoes}%", size=12, color=rt_color, weight=ft.FontWeight.BOLD))

        # Metacritic
        if self.movie.metacritic:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            mc_color = "#FF0000" if self.movie.metacritic < 40 else "#FFCC33" if self.movie.metacritic < 60 else "#66CC33"
            ratings_parts.append(ft.Text("MC ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(str(self.movie.metacritic), size=12, color=mc_color, weight=ft.FontWeight.BOLD))

        if not ratings_parts:
            ratings_parts.append(ft.Text("Нет оценок", size=12, color=COLORS["text_secondary"]))

        return ft.Column(
            controls=[
                ft.Row(controls=ratings_parts, spacing=0),
                ft.Row(
                    controls=[
                        ft.Text("Моя оценка:", size=13, color=COLORS["text_secondary"]),
                        *self._build_star_rating(),
                    ],
                    spacing=4,
                ),
            ],
            spacing=6,
        )

    def _get_star_color(self, rating: int) -> str:
        """Get star color based on rating value - gradient from red to blue."""
        colors = {
            1: "#F44336",   # Red
            2: "#FF5722",   # Deep Orange
            3: "#FF9800",   # Orange
            4: "#FFC107",   # Amber
            5: "#FFEB3B",   # Yellow
            6: "#CDDC39",   # Lime
            7: "#8BC34A",   # Light Green
            8: "#4CAF50",   # Green
            9: "#00BCD4",   # Cyan
            10: "#2196F3",  # Blue
        }
        return colors.get(rating, COLORS["primary"])

    def _build_star_rating(self) -> list[ft.Control]:
        stars = []
        star_color = self._get_star_color(self.user_rating) if self.user_rating else COLORS["star_empty"]

        for i in range(1, 11):
            is_filled = self.user_rating is not None and i <= self.user_rating
            star = ft.IconButton(
                icon=ft.Icons.STAR if is_filled else ft.Icons.STAR_BORDER,
                icon_size=18,
                icon_color=star_color if is_filled else COLORS["star_empty"],
                on_click=lambda e, rating=i: self._handle_rating_click(rating),
                tooltip=str(i),
                padding=0,
                width=24,
                height=24,
            )
            stars.append(star)
        return stars

    def _build_actions_row(self) -> ft.Control:
        review_button_text = "Редактировать рецензию" if self.user_review else "Написать рецензию"

        return ft.Row(
            controls=[
                ft.TextButton(
                    review_button_text,
                    icon=ft.Icons.RATE_REVIEW,
                    on_click=lambda e: self._handle_review_click(),
                    style=ft.ButtonStyle(color=COLORS["primary"]),
                ),
                ft.TextButton(
                    "Найти похожие",
                    icon=ft.Icons.CONTENT_COPY,
                    on_click=lambda e: self._handle_similar_click(),
                    style=ft.ButtonStyle(color=COLORS["text_secondary"]),
                ),
            ],
        )

    def _handle_rating_click(self, rating: int):
        if self.on_rating_change:
            self.on_rating_change(self.movie, rating)

    def _handle_review_click(self):
        if self.on_review_click:
            self.on_review_click(self.movie)

    def _handle_similar_click(self):
        if self.on_similar_click:
            self.on_similar_click(self.movie)
