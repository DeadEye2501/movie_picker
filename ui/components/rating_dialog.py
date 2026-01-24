from typing import Callable, Optional
import flet as ft

from database.models import Movie
from ui.theme import COLORS


class RatingDialog(ft.AlertDialog):
    """Dialog for viewing and editing movie reviews."""

    def __init__(
        self,
        movie: Movie,
        current_review: Optional[str] = None,
        on_save: Optional[Callable[[Movie, str], None]] = None,
        on_close: Optional[Callable[[], None]] = None,
    ):
        self.movie = movie
        self.on_save = on_save
        self.on_close = on_close

        self.review_field = ft.TextField(
            value=current_review or "",
            multiline=True,
            min_lines=5,
            max_lines=10,
            hint_text="Напишите вашу рецензию...",
            bgcolor=COLORS["surface_variant"],
            border_color=COLORS["divider"],
            focused_border_color=COLORS["primary"],
            color=COLORS["text_primary"],
            hint_style=ft.TextStyle(color=COLORS["text_secondary"]),
            cursor_color=COLORS["primary"],
            expand=True,
        )

        super().__init__(
            modal=True,
            title=ft.Text(
                f"Рецензия: {movie.title}",
                size=18,
                weight=ft.FontWeight.BOLD,
                color=COLORS["text_primary"],
            ),
            content=ft.Container(
                content=self.review_field,
                width=500,
                height=200,
            ),
            actions=[
                ft.TextButton(
                    "Отмена",
                    on_click=lambda e: self._close(),
                    style=ft.ButtonStyle(color=COLORS["text_secondary"]),
                ),
                ft.ElevatedButton(
                    "Сохранить",
                    on_click=lambda e: self._save(),
                    bgcolor=COLORS["primary"],
                    color=COLORS["background"],
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            bgcolor=COLORS["surface"],
        )

    def _save(self):
        if self.on_save:
            self.on_save(self.movie, self.review_field.value)
        self._close()

    def _close(self):
        self.open = False
        if self.on_close:
            self.on_close()
        self.update()
