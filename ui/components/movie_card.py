import math
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
        in_wishlist: bool = False,
        ratings_loading: bool = False,
        on_rating_change: Optional[Callable[[Movie, int], None]] = None,
        on_review_click: Optional[Callable[[Movie], None]] = None,
        on_similar_click: Optional[Callable[[Movie], None]] = None,
        on_rating_delete: Optional[Callable[[Movie], None]] = None,
        on_wishlist_toggle: Optional[Callable[[Movie, bool], None]] = None,
        on_person_click: Optional[Callable[[str, str], None]] = None,  # (name, type: 'director'|'actor')
    ):
        self.movie = movie
        self.user_rating = user_rating
        self.user_review = user_review
        self.in_wishlist = in_wishlist
        self.ratings_loading = ratings_loading
        self.on_rating_change = on_rating_change
        self.on_review_click = on_review_click
        self.on_similar_click = on_similar_click
        self.on_rating_delete = on_rating_delete
        self.on_wishlist_toggle = on_wishlist_toggle
        self.on_person_click = on_person_click
        self.description_expanded = False
        self.actors_expanded = False

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
                        self._build_info_text(self.movie.genres_display, "Жанры: "),
                        self._build_clickable_person("Режиссёр: ", self.movie.directors_display, "director"),
                        self._build_clickable_person("Актёры: ", self.movie.actors_display, "actor"),
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

    def _build_clickable_person(self, prefix: str, value: Optional[str], person_type: str) -> ft.Control:
        """Build clickable text for director or actors."""
        if not value:
            return ft.Container(height=0)

        if not self.on_person_click:
            # Fallback to regular text if no callback
            return self._build_info_text(value, prefix)

        # Split names and create clickable spans
        names = [n.strip() for n in value.split(', ') if n.strip()]
        if not names:
            return ft.Container(height=0)

        # For actors with more than 5 names, make expandable
        is_actors = person_type == "actor"
        show_all = self.actors_expanded if is_actors else True
        visible_count = len(names) if show_all else 5
        has_more = len(names) > 5

        spans = [ft.TextSpan(prefix, style=ft.TextStyle(color=COLORS["text_secondary"]))]

        for i, name in enumerate(names[:visible_count]):
            if i > 0:
                spans.append(ft.TextSpan(", ", style=ft.TextStyle(color=COLORS["text_secondary"])))
            spans.append(ft.TextSpan(
                name,
                style=ft.TextStyle(color=COLORS["primary"]),
                on_click=lambda e, n=name, t=person_type: self._handle_person_click(n, t),
            ))

        # Add expand/collapse link for actors
        if is_actors and has_more:
            if self.actors_expanded:
                spans.append(ft.TextSpan(
                    " свернуть",
                    style=ft.TextStyle(color=COLORS["text_secondary"]),
                    on_click=self._toggle_actors,
                ))
            else:
                spans.append(ft.TextSpan(
                    f" и ещё {len(names) - 5}",
                    style=ft.TextStyle(color=COLORS["primary"]),
                    on_click=self._toggle_actors,
                ))

        max_lines = None if (is_actors and self.actors_expanded) else 2
        return ft.Text(spans=spans, size=13, max_lines=max_lines, overflow=ft.TextOverflow.ELLIPSIS)

    def _handle_person_click(self, name: str, person_type: str):
        if self.on_person_click:
            self.on_person_click(name, person_type)

    def _toggle_actors(self, e):
        self.actors_expanded = not self.actors_expanded
        self.content = self._build_content()
        self.update()

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

    def _get_external_url(self) -> Optional[str]:
        """Get URL to external movie database (IMDB preferred, fallback to TMDB)."""
        if self.movie.imdb_id:
            return f"https://www.imdb.com/title/{self.movie.imdb_id}/"
        if self.movie.kinopoisk_id:
            if self.movie.is_tv:
                return f"https://www.themoviedb.org/tv/{self.movie.kinopoisk_id}"
            return f"https://www.themoviedb.org/movie/{self.movie.kinopoisk_id}"
        return None

    def _build_loading_indicator(self) -> ft.Control:
        """Build a small loading indicator for ratings."""
        return ft.Container(
            content=ft.ProgressRing(width=10, height=10, stroke_width=2, color=COLORS["text_secondary"]),
            padding=ft.padding.only(top=2),
        )

    def _build_ratings_row(self) -> ft.Control:
        """Build row with ratings from multiple sources."""
        ratings_parts = []

        # Kinopoisk rating (most relevant for Russian users)
        if self.movie.kp_rating:
            kp_color = self._get_rating_color(self.movie.kp_rating, 10.0)
            ratings_parts.append(ft.Text("КП ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.kp_rating:.1f}", size=12, color=kp_color, weight=ft.FontWeight.BOLD))
        elif self.ratings_loading:
            ratings_parts.append(ft.Text("КП ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(self._build_loading_indicator())

        # IMDB rating
        if self.movie.imdb_rating:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            imdb_color = self._get_rating_color(self.movie.imdb_rating, 10.0)
            ratings_parts.append(ft.Text("IMDB ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.imdb_rating:.1f}", size=12, color=imdb_color, weight=ft.FontWeight.BOLD))
        elif self.ratings_loading:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text("IMDB ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(self._build_loading_indicator())

        # TMDB rating (always available, no loading needed)
        if self.movie.tmdb_rating:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            tmdb_color = self._get_rating_color(self.movie.tmdb_rating, 10.0)
            ratings_parts.append(ft.Text("TMDB ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.tmdb_rating:.1f}", size=12, color=tmdb_color, weight=ft.FontWeight.BOLD))

        # Rotten Tomatoes (0-100 scale)
        if self.movie.rotten_tomatoes:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            rt_color = self._get_rating_color(self.movie.rotten_tomatoes, 100.0)
            ratings_parts.append(ft.Text("RT ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(f"{self.movie.rotten_tomatoes}%", size=12, color=rt_color, weight=ft.FontWeight.BOLD))
        elif self.ratings_loading:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text("RT ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(self._build_loading_indicator())

        # Metacritic (0-100 scale)
        if self.movie.metacritic:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            mc_color = self._get_rating_color(self.movie.metacritic, 100.0)
            ratings_parts.append(ft.Text("MC ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text(str(self.movie.metacritic), size=12, color=mc_color, weight=ft.FontWeight.BOLD))
        elif self.ratings_loading:
            if ratings_parts:
                ratings_parts.append(ft.Text("  ·  ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(ft.Text("MC ", size=12, color=COLORS["text_secondary"]))
            ratings_parts.append(self._build_loading_indicator())

        if not ratings_parts:
            if self.ratings_loading:
                ratings_parts.append(ft.Text("Загрузка рейтингов ", size=12, color=COLORS["text_secondary"]))
                ratings_parts.append(self._build_loading_indicator())
            else:
                ratings_parts.append(ft.Text("Нет оценок", size=12, color=COLORS["text_secondary"]))

        # Add external link button (IMDB preferred, fallback to TMDB)
        external_url = self._get_external_url()
        if external_url:
            ratings_parts.append(ft.Container(width=8))  # Spacer
            ratings_parts.append(
                ft.IconButton(
                    icon=ft.Icons.OPEN_IN_NEW,
                    icon_size=14,
                    icon_color=COLORS["text_secondary"],
                    tooltip="Открыть на IMDB/TMDB",
                    padding=0,
                    width=20,
                    height=20,
                    url=external_url,
                )
            )

        # Build user rating row with optional label
        user_rating_controls = [
            ft.Text("Моя оценка:", size=13, color=COLORS["text_secondary"]),
            *self._build_star_rating(),
        ]
        if self.user_rating is not None:
            user_rating_controls.append(
                ft.Text(
                    self._get_rating_label(self.user_rating),
                    size=13,
                    weight=ft.FontWeight.BOLD,
                    color=self._get_star_color(self.user_rating),
                )
            )

        return ft.Column(
            controls=[
                ft.Row(controls=ratings_parts, spacing=0),
                ft.Row(controls=user_rating_controls, spacing=4),
            ],
            spacing=6,
        )

    def _get_rating_color(self, rating: float, max_value: float = 10.0) -> str:
        """Get color based on rating value - gradient from red to blue.

        Works for any scale (1-10, 0-100, etc.) by normalizing to 1-10.
        Scale: 0.1-1 = red, 1.1-2 = deep orange, ..., 9.1-10 = blue
        """
        # Normalize to 1-10 scale
        normalized = (rating / max_value) * 10

        # Round up to get the color bucket (7.1 -> 8 -> green)
        color_index = math.ceil(normalized)
        color_index = max(1, min(10, color_index))  # Clamp to 1-10

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

        return colors[color_index]

    def _get_star_color(self, rating: int) -> str:
        """Get star color based on rating value - gradient from red to blue."""
        return self._get_rating_color(float(rating), 10.0)

    def _get_rating_label(self, rating: int) -> str:
        """Get text label for rating value."""
        labels = {
            1: "Отвратительно",
            2: "Плохо",
            3: "Уныло",
            4: "Посредственно",
            5: "Средне",
            6: "Неплохо",
            7: "Интересно",
            8: "Хорошо",
            9: "Отлично",
            10: "Шедевр",
        }
        return labels.get(rating, "")

    def _build_star_rating(self) -> list[ft.Control]:
        stars = []
        star_color = self._get_star_color(self.user_rating) if self.user_rating else COLORS["star_empty"]

        rating_labels = {
            1: "1 — Отвратительно",
            2: "2 — Плохо",
            3: "3 — Уныло",
            4: "4 — Посредственно",
            5: "5 — Средне",
            6: "6 — Неплохо",
            7: "7 — Интересно",
            8: "8 — Хорошо",
            9: "9 — Отлично",
            10: "10 — Шедевр",
        }

        for i in range(1, 11):
            is_filled = self.user_rating is not None and i <= self.user_rating
            star = ft.IconButton(
                icon=ft.Icons.STAR if is_filled else ft.Icons.STAR_BORDER,
                icon_size=18,
                icon_color=star_color if is_filled else COLORS["star_empty"],
                on_click=lambda e, rating=i: self._handle_rating_click(rating),
                tooltip=rating_labels[i],
                padding=0,
                width=24,
                height=24,
            )
            stars.append(star)

        # Add delete button if there's a rating
        if self.user_rating is not None:
            delete_button = ft.IconButton(
                icon=ft.Icons.DELETE_OUTLINE,
                icon_size=18,
                icon_color=COLORS["text_secondary"],
                on_click=lambda e: self._handle_rating_delete(),
                tooltip="Удалить оценку",
                padding=0,
                width=24,
                height=24,
            )
            stars.append(delete_button)

        return stars

    def _build_actions_row(self) -> ft.Control:
        review_button_text = "Редактировать рецензию" if self.user_review else "Написать рецензию"

        controls = [
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
        ]

        # Show wishlist button only for unrated movies
        if self.user_rating is None:
            wishlist_text = "В списке" if self.in_wishlist else "Хочу посмотреть"
            wishlist_icon = ft.Icons.BOOKMARK if self.in_wishlist else ft.Icons.BOOKMARK_BORDER
            controls.append(
                ft.TextButton(
                    wishlist_text,
                    icon=wishlist_icon,
                    on_click=lambda e: self._handle_wishlist_toggle(),
                    style=ft.ButtonStyle(color=COLORS["primary"] if self.in_wishlist else COLORS["text_secondary"]),
                )
            )

        return ft.Row(controls=controls)

    def _handle_rating_click(self, rating: int):
        if self.on_rating_change:
            self.on_rating_change(self.movie, rating)

    def _handle_rating_delete(self):
        if self.on_rating_delete:
            self.on_rating_delete(self.movie)

    def _handle_review_click(self):
        if self.on_review_click:
            self.on_review_click(self.movie)

    def _handle_similar_click(self):
        if self.on_similar_click:
            self.on_similar_click(self.movie)

    def _handle_wishlist_toggle(self):
        if self.on_wishlist_toggle:
            self.on_wishlist_toggle(self.movie, not self.in_wishlist)
