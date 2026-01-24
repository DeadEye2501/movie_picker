from typing import Callable, Optional
import flet as ft

from ui.theme import COLORS


# Available genres with TMDB IDs
GENRES = [
    ("Боевик", 28),
    ("Приключения", 12),
    ("Мультфильм", 16),
    ("Комедия", 35),
    ("Криминал", 80),
    ("Документальный", 99),
    ("Драма", 18),
    ("Семейный", 10751),
    ("Фэнтези", 14),
    ("История", 36),
    ("Ужасы", 27),
    ("Музыка", 10402),
    ("Детектив", 9648),
    ("Мелодрама", 10749),
    ("Фантастика", 878),
    ("ТВ фильм", 10770),
    ("Триллер", 53),
    ("Военный", 10752),
    ("Вестерн", 37),
]


class SearchBar(ft.Container):
    """Search bar component with action buttons and genre filter."""

    def __init__(
        self,
        on_search: Optional[Callable[[str, list[int]], None]] = None,
        on_my_ratings: Optional[Callable[[], None]] = None,
        on_magic: Optional[Callable[[], None]] = None,
        on_genre_change: Optional[Callable[[], None]] = None,
    ):
        self.on_search = on_search
        self.on_my_ratings = on_my_ratings
        self.on_magic = on_magic
        self.on_genre_change = on_genre_change
        self.selected_genres: list[int] = []

        self.clear_icon = ft.Container(
            content=ft.Icon(ft.Icons.CLOSE, size=14, color=COLORS["text_secondary"]),
            on_click=self._clear_search_text,
            width=20,
            height=20,
            border_radius=10,
            visible=False,
        )

        self.search_field = ft.TextField(
            hint_text="Поиск фильмов...",
            expand=True,
            border_radius=18,
            bgcolor=COLORS["surface_variant"],
            border_color=COLORS["divider"],
            focused_border_color=COLORS["primary"],
            color=COLORS["text_primary"],
            hint_style=ft.TextStyle(color=COLORS["text_secondary"]),
            cursor_color=COLORS["primary"],
            content_padding=ft.padding.symmetric(horizontal=14, vertical=6),
            text_size=14,
            height=36,
            on_submit=lambda e: self._handle_search(),
            on_change=self._on_text_change,
            suffix=self.clear_icon,
        )

        self.genre_button = ft.IconButton(
            icon=ft.Icons.FILTER_LIST,
            icon_size=20,
            icon_color=COLORS["text_primary"],
            bgcolor=COLORS["surface_variant"],
            on_click=self._show_genre_menu,
            tooltip="Жанры",
            width=36,
            height=36,
        )

        self.selected_genres_text = ft.Text(
            "",
            size=11,
            color=COLORS["text_secondary"],
            max_lines=1,
            overflow=ft.TextOverflow.ELLIPSIS,
        )

        # Ratings button (becomes sort button in ratings mode)
        self.ratings_button = None  # Will be built in _build_ratings_button
        self.ratings_icon_container = None

        super().__init__(
            content=self._build_content(),
            margin=ft.margin.only(bottom=8),
        )

    def _build_content(self) -> ft.Control:
        return ft.Column(
            controls=[
                ft.Row(
                    controls=[
                        self.search_field,
                        self.genre_button,
                        ft.IconButton(
                            icon=ft.Icons.SEARCH,
                            icon_size=20,
                            icon_color=COLORS["background"],
                            bgcolor=COLORS["primary"],
                            on_click=lambda e: self._handle_search(),
                            tooltip="Найти",
                            width=36,
                            height=36,
                        ),
                        ft.IconButton(
                            icon=ft.Icons.AUTO_AWESOME,
                            icon_size=20,
                            icon_color=COLORS["primary"],
                            bgcolor=COLORS["surface_variant"],
                            on_click=lambda e: self._handle_magic(),
                            tooltip="Подобрать фильм",
                            width=36,
                            height=36,
                        ),
                        self._build_ratings_button(),
                    ],
                    spacing=8,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                self.selected_genres_text,
            ],
            spacing=2,
        )

    def _show_genre_menu(self, e):
        """Show genre selection dialog."""
        checkboxes = []
        for name, genre_id in GENRES:
            cb = ft.Checkbox(
                label=name,
                value=genre_id in self.selected_genres,
                data=genre_id,
                fill_color={
                    ft.ControlState.SELECTED: COLORS["primary"],
                },
                check_color=COLORS["background"],
            )
            checkboxes.append(cb)

        def close_dialog(e):
            dialog.open = False
            self.page.update()

        def apply_selection(e):
            self.selected_genres = [cb.data for cb in checkboxes if cb.value]
            self._update_selected_genres_text()
            self.genre_button.icon_color = COLORS["primary"] if self.selected_genres else COLORS["text_primary"]
            dialog.open = False
            self.page.update()
            if self.on_genre_change:
                self.on_genre_change()

        def clear_selection(e):
            for cb in checkboxes:
                cb.value = False
            self.selected_genres = []
            self._update_selected_genres_text()
            self.genre_button.icon_color = COLORS["text_primary"]
            dialog.open = False
            self.page.update()
            if self.on_genre_change:
                self.on_genre_change()

        dialog = ft.AlertDialog(
            title=ft.Text("Выберите жанры", size=16),
            content=ft.Container(
                content=ft.Column(
                    controls=checkboxes,
                    scroll=ft.ScrollMode.AUTO,
                    spacing=0,
                ),
                width=250,
            ),
            actions=[
                ft.TextButton(
                    "Сбросить",
                    on_click=clear_selection,
                    style=ft.ButtonStyle(color=COLORS["text_secondary"]),
                ),
                ft.TextButton(
                    "Применить",
                    on_click=apply_selection,
                    style=ft.ButtonStyle(color=COLORS["primary"]),
                ),
            ],
            bgcolor=COLORS["surface"],
        )

        self.page.overlay.append(dialog)
        dialog.open = True
        self.page.update()

    def _update_selected_genres_text(self):
        """Update the text showing selected genres."""
        if self.selected_genres:
            names = [name for name, gid in GENRES if gid in self.selected_genres]
            self.selected_genres_text.value = ", ".join(names)
        else:
            self.selected_genres_text.value = ""

    def _on_text_change(self, e):
        """Show/hide clear button based on text content."""
        has_text = bool(self.search_field.value)
        if self.clear_icon.visible != has_text:
            self.clear_icon.visible = has_text
            self.search_field.update()

    def _clear_search_text(self, e):
        """Clear the search text."""
        self.search_field.value = ""
        self.clear_icon.visible = False
        self.search_field.update()

    def _handle_search(self):
        query = self.search_field.value or ""
        if self.on_search and (query or self.selected_genres):
            self.on_search(query, self.selected_genres)

    def _handle_my_ratings(self):
        if self.on_my_ratings:
            self.on_my_ratings()

    def _handle_magic(self):
        if self.on_magic:
            self.on_magic()

    def get_query(self) -> str:
        return self.search_field.value or ""

    def get_selected_genres(self) -> list[int]:
        return self.selected_genres

    def get_selected_genre_names(self) -> list[str]:
        """Get selected genre names (for filtering rated movies)."""
        return [name for name, gid in GENRES if gid in self.selected_genres]

    def _build_ratings_button(self) -> ft.Container:
        """Build the ratings/sort button."""
        self.ratings_icon_container = ft.Row(
            controls=[ft.Icon(ft.Icons.STAR, size=20, color=COLORS["text_primary"])],
            alignment=ft.MainAxisAlignment.CENTER,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=0,
        )
        self.ratings_button = ft.Container(
            content=self.ratings_icon_container,
            width=36,
            height=36,
            border_radius=18,
            bgcolor=COLORS["surface_variant"],
            on_click=lambda e: self._handle_my_ratings(),
            tooltip="Мои оценки",
            alignment=ft.Alignment(0, 0),
        )
        return self.ratings_button

    def set_ratings_button_icons(self, main_icon, arrow_icon):
        """Set icons for ratings button (sort mode)."""
        self.ratings_icon_container.controls = [
            ft.Icon(main_icon, size=16, color=COLORS["primary"]),
            ft.Icon(arrow_icon, size=14, color=COLORS["primary"]),
        ]
        self.ratings_button.tooltip = "Сортировка"
        self.ratings_icon_container.update()

    def reset_ratings_button(self):
        """Reset ratings button to default state."""
        self.ratings_icon_container.controls = [
            ft.Icon(ft.Icons.STAR, size=20, color=COLORS["text_primary"])
        ]
        self.ratings_button.tooltip = "Мои оценки"
        self.ratings_icon_container.update()

    def clear(self):
        self.search_field.value = ""
        self.clear_icon.visible = False
        self.selected_genres = []
        self._update_selected_genres_text()
        self.genre_button.icon_color = COLORS["text_primary"]
        self.update()
