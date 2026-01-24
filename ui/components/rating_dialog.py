from typing import Callable, Optional
import flet as ft

from database.models import Movie
from ui.theme import COLORS


def show_rating_dialog(
    page: ft.Page,
    movie: Movie,
    current_review: Optional[str] = None,
    on_save: Optional[Callable[[Movie, str], None]] = None,
):
    """Show a dialog for editing movie review."""

    review_field = ft.TextField(
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

    def close_dialog(e=None):
        dialog.open = False
        page.update()

    def save_and_close(e):
        if on_save:
            on_save(movie, review_field.value)
        close_dialog()

    dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text(
            f"Рецензия: {movie.title}",
            size=18,
            weight=ft.FontWeight.BOLD,
            color=COLORS["text_primary"],
        ),
        content=ft.Container(
            content=review_field,
            width=500,
            height=200,
        ),
        actions=[
            ft.TextButton(
                "Отмена",
                on_click=close_dialog,
                style=ft.ButtonStyle(color=COLORS["text_secondary"]),
            ),
            ft.ElevatedButton(
                "Сохранить",
                on_click=save_and_close,
                bgcolor=COLORS["primary"],
                color=COLORS["background"],
            ),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
        bgcolor=COLORS["surface"],
    )

    page.overlay.append(dialog)
    dialog.open = True
    page.update()


# Keep class for backwards compatibility
class RatingDialog(ft.AlertDialog):
    """Deprecated. Use show_rating_dialog() instead."""
    pass
