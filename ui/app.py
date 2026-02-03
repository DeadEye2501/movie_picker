import os
import sys
import asyncio

import flet as ft

from api import TMDBAPI, OMDBAPI, KinopoiskAPI, MDBListAPI
from database import (
    init_db, close_db, get_session, save_user_rating, delete_user_rating,
    update_entity_ratings_for_movie, get_all_user_ratings_filtered, get_user_rating,
    get_user_ratings_batch,
    is_in_wishlist, add_to_wishlist, remove_from_wishlist, get_wishlist, get_wishlist_movie_ids,
    get_all_tags, create_tag, rename_tag, delete_tag, set_movie_tags, get_movie_tags,
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
        ("date_desc", ft.Icons.SCHEDULE, ft.Icons.ARROW_DOWNWARD),
        ("date_asc", ft.Icons.SCHEDULE, ft.Icons.ARROW_UPWARD),
    ]

    def __init__(self, tmdb_api_key: str, omdb_api_key: str = None, kp_api_key: str = None, mdblist_api_key: str = None, db_path: str = "movie_picker.db"):
        self.db_path = db_path
        self.page: ft.Page = None
        self.search_bar: SearchBar = None
        self.movie_list: MovieList = None
        self.is_ratings_mode = False
        self.is_wishlist_mode = False
        self.is_stats_mode = False
        self.sort_state_index = 0
        self._selected_tag_ids: set[int] = set()
        self._excluded_tag_ids: set[int] = set()
        self._selected_rating_values: set[int] = set()
        # Search pagination state
        self._search_query: str = ""
        self._search_genres: list[int] = []
        self._search_next_page: int = 4  # first search loads pages 1-3

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
                
                # Give background tasks a moment to see the shutdown flag
                await asyncio.sleep(0.2)
                
                # Close API clients
                try:
                    await self.tmdb_api.close()
                    if self.omdb_api:
                        await self.omdb_api.close()
                    if self.kp_api:
                        await self.kp_api.close()
                    if self.mdblist_api:
                        await self.mdblist_api.close()
                    await self.search_service.close()
                except Exception:
                    pass
                
                # Close database connections
                try:
                    await close_db()
                except Exception:
                    pass
                
                # Exit cleanly
                import sys
                sys.exit(0)

        page.window.on_event = on_window_event

        self.search_bar = SearchBar(
            on_search=self._handle_search,
            on_my_ratings=self._handle_my_ratings,
            on_wishlist=self._handle_wishlist,
            on_magic=self._handle_magic,
            on_genre_change=self._handle_genre_change,
            on_collapse_all=self._handle_collapse_all,
            on_manage_tags=self._handle_manage_tags,
            on_rating_filter=self._handle_rating_filter,
            on_stats=self._handle_stats,
        )

        self.movie_list = MovieList(
            on_rating_change=self._handle_rating_change,
            on_review_click=self._handle_review_click,
            on_similar_click=self._handle_similar_click,
            on_rating_delete=self._handle_rating_delete,
            on_wishlist_toggle=self._handle_wishlist_toggle,
            on_person_click=self._handle_person_click,
            on_tags_click=self._handle_tags_click,
            on_fetch_more=self._handle_fetch_more,
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
        self._exit_stats_mode()
        self._show_loading()
        # Store search context for pagination
        self._search_query = query
        self._search_genres = genres or []
        self._search_next_page = 4  # pages 1-3 loaded initially
        self.movie_list.on_fetch_more = self._handle_fetch_more

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

    def _handle_fetch_more(self):
        """Handle 'load more' when all local results are shown — fetch next API pages."""
        async def do_fetch():
            if is_shutting_down():
                return
            try:
                async with get_session() as session:
                    movies = await self.search_service.search_movies(
                        session,
                        self._search_query,
                        genres=self._search_genres,
                        skip_ratings=True,
                        start_page=self._search_next_page,
                        num_pages=3,
                    )
                    self._search_next_page += 3

                    if is_shutting_down():
                        return

                    if movies:
                        ratings = await self._get_ratings_for_movies(movies)
                        wishlist_ids = await get_wishlist_movie_ids(session)
                        self.movie_list.append_movies(movies, ratings, wishlist_ids)
                    else:
                        # No more results — disable further fetching
                        self.movie_list.on_fetch_more = None
                        self.movie_list._fetching_more = False
                        self.movie_list._remove_load_more_row()
                        self.movie_list.movies_column.update()
            except Exception:
                self.movie_list._fetching_more = False
                self.movie_list._remove_load_more_row()
                self.movie_list._append_load_more_if_needed()
                self.movie_list.movies_column.update()

        self.page.run_task(do_fetch)

    def _handle_my_ratings(self):
        """Handle my ratings button click - toggle sort or enter ratings mode."""
        self._exit_wishlist_mode()
        self._exit_stats_mode()
        self.movie_list.on_fetch_more = None  # No API pagination for ratings
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
        self._exit_stats_mode()
        self.movie_list.on_fetch_more = None  # No API pagination for wishlist
        if self.is_wishlist_mode:
            # Exit wishlist mode
            self._exit_wishlist_mode()
        else:
            # Enter wishlist mode
            self.is_wishlist_mode = True
            self.search_bar.set_wishlist_active(True)
            self.page.run_task(self._load_wishlist)

    def _handle_collapse_all(self):
        """Handle collapse/expand all button click."""
        self.movie_list.toggle_all_collapsed()

    def _handle_genre_change(self):
        """Handle genre filter change."""
        if self.is_stats_mode:
            self.page.run_task(self._load_stats)
            return
        if not self.is_ratings_mode:
            self.is_ratings_mode = True
            self._exit_wishlist_mode()
            self.sort_state_index = 0
            self._update_sort_button()
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

    def _exit_stats_mode(self):
        """Exit stats mode."""
        self.is_stats_mode = False

    async def _load_filtered_ratings(self):
        """Load user ratings with current sort, genre and tag filter applied."""
        try:
            async with get_session() as session:
                sort_key = self.SORT_STATES[self.sort_state_index][0]
                genres = self.search_bar.get_selected_genre_names()

                # Resolve selected/excluded tag IDs to names
                tag_names = None
                exclude_tag_names = None
                if self._selected_tag_ids or self._excluded_tag_ids:
                    all_tags = await get_all_tags(session)
                    if self._selected_tag_ids:
                        tag_names = [t.name for t in all_tags if t.id in self._selected_tag_ids]
                    if self._excluded_tag_ids:
                        exclude_tag_names = [t.name for t in all_tags if t.id in self._excluded_tag_ids]

                user_ratings = await get_all_user_ratings_filtered(
                    session,
                    sort_by=sort_key,
                    genres=genres if genres else None,
                    tags=tag_names if tag_names else None,
                    exclude_tags=exclude_tag_names if exclude_tag_names else None,
                    rating_values=self._selected_rating_values if self._selected_rating_values else None,
                )

                if not user_ratings:
                    if genres or tag_names or exclude_tag_names or self._selected_rating_values:
                        self.movie_list.set_message("Нет фильмов с выбранными фильтрами")
                    else:
                        self.movie_list.set_message("Вы ещё не оценили ни одного фильма")
                else:
                    movies = [ur.movie for ur in user_ratings]
                    ratings = {ur.movie_id: ur for ur in user_ratings}
                    wishlist_ids = await get_wishlist_movie_ids(session)
                    # Build movie tags map
                    movie_tags = {}
                    for ur in user_ratings:
                        if hasattr(ur.movie, 'tag_list') and ur.movie.tag_list:
                            movie_tags[ur.movie_id] = [t.name for t in ur.movie.tag_list]
                    self.movie_list.movie_tags = movie_tags
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
        self._exit_stats_mode()
        self.movie_list.on_fetch_more = None  # Magic returns a single movie, no pagination
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
                if is_shutting_down():
                    return
                # Revert on error
                try:
                    self.movie_list.update_rating(movie.id, None)
                    self.page.update()
                except Exception:
                    pass  # Ignore if UI is destroyed

        self.page.run_task(do_save)

    def _handle_rating_delete(self, movie: Movie):
        """Handle rating deletion for a movie."""
        # Optimistic UI update
        self.movie_list.remove_rating(movie.id, remove_from_list=self.is_ratings_mode)
        self.page.update()

        # Delete from DB in background
        async def do_delete():
            try:
                async with get_session() as session:
                    deleted = await delete_user_rating(session, movie.id)
                    if not deleted:
                        return
                # Update entity ratings in background
                async with get_session() as session:
                    await update_entity_ratings_for_movie(session, movie.id)
            except Exception as e:
                if is_shutting_down():
                    return
                pass

        self.page.run_task(do_delete)

    def _handle_wishlist_toggle(self, movie: Movie, add: bool):
        """Handle wishlist toggle for a movie."""
        # Optimistic UI update
        if add:
            self.movie_list.update_wishlist(movie.id, True)
        else:
            if self.is_wishlist_mode:
                self.movie_list.remove_from_wishlist_view(movie.id)
            else:
                self.movie_list.update_wishlist(movie.id, False)
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
                if is_shutting_down():
                    return
                # Revert on error
                try:
                    if add:
                        self.movie_list.update_wishlist(movie.id, False)
                    else:
                        self.movie_list.update_wishlist(movie.id, True)
                    self.page.update()
                except Exception:
                    pass

        self.page.run_task(do_toggle)

    def _handle_person_click(self, name: str, person_type: str):
        """Handle click on director or actor name - search for their movies."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._exit_stats_mode()
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
            except Exception:
                pass

        self.page.run_task(do_show)

    def _handle_manage_tags(self):
        """Handle global tag management button click."""
        async def do_show():
            try:
                async with get_session() as session:
                    all_tags = await get_all_tags(session)
                    self._show_manage_tags_dialog(all_tags)
            except Exception:
                pass

        self.page.run_task(do_show)

    def _show_manage_tags_dialog(self, all_tags):
        """Show dialog for filtering, creating, renaming and deleting tags."""
        tags_column = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO)

        # Tri-state icons: 0=unchecked, 1=include (check), 2=exclude (cross)
        _TRISTATE_ICONS = {
            0: (ft.Icons.CHECK_BOX_OUTLINE_BLANK, COLORS["text_secondary"]),
            1: (ft.Icons.CHECK_BOX, COLORS["primary"]),
            2: (ft.Icons.DISABLED_BY_DEFAULT, "#F44336"),
        }

        def _cycle_tristate(e):
            btn = e.control
            state = (btn.data["state"] + 1) % 3
            btn.data["state"] = state
            icon, color = _TRISTATE_ICONS[state]
            btn.icon = icon
            btn.icon_color = color
            btn.update()

        def _get_initial_state(tag_id: int) -> int:
            if tag_id in self._selected_tag_ids:
                return 1
            if tag_id in self._excluded_tag_ids:
                return 2
            return 0

        def build_tag_row(tag_id: int, tag_name: str, initial_state: int) -> ft.Row:
            """Build a tag row: tri-state filter button + editable name + delete."""
            icon, color = _TRISTATE_ICONS[initial_state]
            tristate_btn = ft.IconButton(
                icon=icon,
                icon_color=color,
                icon_size=20,
                data={"tag_id": tag_id, "state": initial_state},
                on_click=_cycle_tristate,
                padding=0,
                width=32,
                height=32,
                tooltip="Нажмите для переключения: нет фильтра → включить → исключить",
            )
            name_field = ft.TextField(
                value=tag_name,
                bgcolor=COLORS["surface_variant"],
                border_color=COLORS["divider"],
                focused_border_color=COLORS["primary"],
                color=COLORS["text_primary"],
                cursor_color=COLORS["primary"],
                content_padding=ft.padding.symmetric(horizontal=10, vertical=4),
                text_size=13,
                height=36,
                expand=True,
                data={"id": tag_id, "original": tag_name},
            )
            return ft.Row(
                controls=[
                    tristate_btn,
                    name_field,
                    ft.IconButton(
                        icon=ft.Icons.DELETE_OUTLINE,
                        icon_size=18,
                        icon_color=COLORS["text_secondary"],
                        tooltip="Удалить тег",
                        on_click=lambda e: None,  # wired below
                        padding=0,
                        width=32,
                        height=32,
                    ),
                ],
                spacing=4,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            )

        for tag in all_tags:
            initial_state = _get_initial_state(tag.id)
            row = build_tag_row(tag.id, tag.name, initial_state)
            row.controls[2].on_click = lambda e, r=row, tid=tag.id: _delete_tag(r, tid)
            tags_column.controls.append(row)

        new_tag_field = ft.TextField(
            hint_text="Новый тег...",
            bgcolor=COLORS["surface_variant"],
            border_color=COLORS["divider"],
            focused_border_color=COLORS["primary"],
            color=COLORS["text_primary"],
            hint_style=ft.TextStyle(color=COLORS["text_secondary"]),
            cursor_color=COLORS["primary"],
            content_padding=ft.padding.symmetric(horizontal=10, vertical=4),
            text_size=13,
            height=36,
            expand=True,
            on_submit=lambda e: _add_new_tag(e),
        )

        def close_dialog(e=None):
            dialog.open = False
            self.page.update()

        def _delete_tag(row, tag_id):
            async def do_delete():
                try:
                    async with get_session() as session:
                        await delete_tag(session, tag_id)
                        if row in tags_column.controls:
                            tags_column.controls.remove(row)
                        self._selected_tag_ids.discard(tag_id)
                        self.page.update()
                except Exception:
                    pass
            self.page.run_task(do_delete)

        def _add_new_tag(e):
            name = new_tag_field.value.strip() if new_tag_field.value else ""
            if not name:
                return

            async def do_create():
                try:
                    async with get_session() as session:
                        tag = await create_tag(session, name)
                        row = build_tag_row(tag.id, tag.name, 0)
                        row.controls[2].on_click = lambda ev, r=row, tid=tag.id: _delete_tag(r, tid)
                        tags_column.controls.append(row)
                        new_tag_field.value = ""
                        no_tags_text.visible = False
                        self.page.update()
                except Exception:
                    pass
            self.page.run_task(do_create)

        def _clear_filter(e):
            # Reset all tri-state buttons, apply immediately and close
            for row in tags_column.controls:
                if isinstance(row, ft.Row) and row.controls:
                    btn = row.controls[0]
                    if isinstance(btn, ft.IconButton) and isinstance(btn.data, dict) and "state" in btn.data:
                        btn.data["state"] = 0
                        icon, color = _TRISTATE_ICONS[0]
                        btn.content = ft.Icon(icon, size=20, color=color)
            _apply(e)

        def _apply(e):
            """Save renames, apply tag filter, close."""
            # Collect selected and excluded tag ids for filter
            new_selected = set()
            new_excluded = set()
            rename_tasks = []
            for row in tags_column.controls:
                if isinstance(row, ft.Row) and len(row.controls) >= 2:
                    btn = row.controls[0]
                    name_field = row.controls[1]
                    if isinstance(btn, ft.IconButton) and isinstance(btn.data, dict) and "state" in btn.data:
                        tag_id = btn.data["tag_id"]
                        state = btn.data["state"]
                        if state == 1:
                            new_selected.add(tag_id)
                        elif state == 2:
                            new_excluded.add(tag_id)
                    if isinstance(name_field, ft.TextField) and name_field.data:
                        tag_id = name_field.data["id"]
                        original = name_field.data["original"]
                        current = name_field.value.strip() if name_field.value else ""
                        if current and current != original:
                            rename_tasks.append((tag_id, current))

            self._selected_tag_ids = new_selected
            self._excluded_tag_ids = new_excluded
            # Update tag button appearance
            has_filter = new_selected or new_excluded
            self.search_bar.tag_button.icon = ft.Icons.LABEL if has_filter else ft.Icons.LABEL_OUTLINE
            self.search_bar.tag_button.icon_color = COLORS["primary"] if has_filter else COLORS["text_primary"]
            self.search_bar.tag_button.update()

            async def do_save():
                try:
                    if rename_tasks:
                        async with get_session() as session:
                            for tag_id, new_name in rename_tasks:
                                await rename_tag(session, tag_id, new_name)
                    if not is_shutting_down():
                        if self.is_stats_mode:
                            await self._load_stats()
                        else:
                            if not self.is_ratings_mode:
                                self.is_ratings_mode = True
                                self._exit_wishlist_mode()
                                self.sort_state_index = 0
                                self._update_sort_button()
                            await self._load_filtered_ratings()
                except Exception:
                    pass
            self.page.run_task(do_save)
            close_dialog()

        new_tag_row = ft.Row(
            controls=[
                new_tag_field,
                ft.IconButton(
                    icon=ft.Icons.ADD,
                    icon_size=18,
                    icon_color=COLORS["primary"],
                    on_click=_add_new_tag,
                    tooltip="Добавить тег",
                    width=32,
                    height=32,
                ),
            ],
            spacing=4,
        )

        no_tags_text = ft.Text(
            "Нет тегов. Создайте первый тег ниже.",
            size=13,
            color=COLORS["text_secondary"],
            visible=len(all_tags) == 0,
        )

        dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Теги", size=16, weight=ft.FontWeight.BOLD, color=COLORS["text_primary"]),
            content=ft.Container(
                content=ft.Column(
                    controls=[no_tags_text, tags_column, ft.Divider(color=COLORS["divider"]), new_tag_row],
                    spacing=8,
                ),
                width=380,
                height=350,
            ),
            actions=[
                ft.TextButton("Сбросить", on_click=_clear_filter, style=ft.ButtonStyle(color=COLORS["text_secondary"])),
                ft.TextButton("Отмена", on_click=close_dialog, style=ft.ButtonStyle(color=COLORS["text_secondary"])),
                ft.ElevatedButton("Применить", on_click=_apply, bgcolor=COLORS["primary"], color=COLORS["background"]),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            bgcolor=COLORS["surface"],
        )

        self.page.overlay.append(dialog)
        dialog.open = True
        self.page.update()

    def _handle_rating_filter(self):
        """Handle rating filter button click."""
        self._show_rating_filter_dialog()

    def _show_rating_filter_dialog(self):
        """Show dialog for filtering by user rating (OR logic)."""
        RATING_COLORS = {
            1: "#F44336", 2: "#FF5722", 3: "#FF9800", 4: "#FFC107", 5: "#FFEB3B",
            6: "#CDDC39", 7: "#8BC34A", 8: "#4CAF50", 9: "#00BCD4", 10: "#2196F3",
        }
        RATING_LABELS = {
            1: "Отвратительно", 2: "Плохо", 3: "Уныло", 4: "Посредственно", 5: "Средне",
            6: "Неплохо", 7: "Интересно", 8: "Хорошо", 9: "Отлично", 10: "Шедевр",
        }

        checkboxes = []
        rows = []
        for value in range(10, 0, -1):
            color = RATING_COLORS[value]
            stars = [
                ft.Icon(ft.Icons.STAR, size=14, color=color)
                for _ in range(value)
            ] + [
                ft.Icon(ft.Icons.STAR_BORDER, size=14, color=COLORS["star_empty"])
                for _ in range(10 - value)
            ]

            cb = ft.Checkbox(
                value=value in self._selected_rating_values,
                data=value,
                fill_color={ft.ControlState.SELECTED: color},
                check_color=COLORS["background"],
            )
            checkboxes.append(cb)

            row = ft.Row(
                controls=[
                    cb,
                    *stars,
                    ft.Text(
                        f" — {RATING_LABELS[value]}",
                        size=11,
                        color=color,
                        weight=ft.FontWeight.BOLD,
                    ),
                ],
                spacing=0,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            )
            rows.append(row)

        def close_dialog(e=None):
            dialog.open = False
            self.page.update()

        def _clear_and_apply(e):
            for cb in checkboxes:
                cb.value = False
            _apply(e)

        def _apply(e):
            self._selected_rating_values = {cb.data for cb in checkboxes if cb.value}
            has_filter = bool(self._selected_rating_values)
            self.search_bar.rating_filter_button.icon = ft.Icons.STAR if has_filter else ft.Icons.STAR_BORDER
            self.search_bar.rating_filter_button.icon_color = COLORS["primary"] if has_filter else COLORS["text_primary"]
            self.search_bar.rating_filter_button.update()

            if self.is_stats_mode:
                self.page.run_task(self._load_stats)
            else:
                if not self.is_ratings_mode:
                    self.is_ratings_mode = True
                    self._exit_wishlist_mode()
                    self.sort_state_index = 0
                    self._update_sort_button()
                self.page.run_task(self._load_filtered_ratings)
            close_dialog()

        dialog = ft.AlertDialog(
            title=ft.Text("Фильтр по оценке", size=16, weight=ft.FontWeight.BOLD, color=COLORS["text_primary"]),
            content=ft.Container(
                content=ft.Column(
                    controls=rows,
                    scroll=ft.ScrollMode.AUTO,
                    spacing=0,
                ),
            ),
            actions=[
                ft.TextButton("Сбросить", on_click=_clear_and_apply, style=ft.ButtonStyle(color=COLORS["text_secondary"])),
                ft.ElevatedButton("Применить", on_click=_apply, bgcolor=COLORS["primary"], color=COLORS["background"]),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            bgcolor=COLORS["surface"],
        )

        self.page.overlay.append(dialog)
        dialog.open = True
        self.page.update()

    def _handle_tags_click(self, movie: Movie):
        """Handle per-movie tags button click - assign/unassign existing tags."""
        async def do_show():
            try:
                async with get_session() as session:
                    all_tags = await get_all_tags(session)
                    movie_tag_list = await get_movie_tags(session, movie.id)
                    movie_tag_ids = {t.id for t in movie_tag_list}
                    self._show_movie_tags_dialog(movie, all_tags, movie_tag_ids)
            except Exception:
                pass

        self.page.run_task(do_show)

    def _show_movie_tags_dialog(self, movie: Movie, all_tags, movie_tag_ids: set):
        """Show simple dialog for assigning existing tags to a movie."""
        if not all_tags:
            # No tags exist — prompt user to create them via global button
            dialog = ft.AlertDialog(
                title=ft.Text("Нет тегов", size=16, weight=ft.FontWeight.BOLD, color=COLORS["text_primary"]),
                content=ft.Text(
                    "Создайте теги через кнопку управления тегами в панели поиска.",
                    size=13,
                    color=COLORS["text_secondary"],
                ),
                actions=[
                    ft.TextButton("OK", on_click=lambda e: _close(), style=ft.ButtonStyle(color=COLORS["primary"])),
                ],
                bgcolor=COLORS["surface"],
            )

            def _close():
                dialog.open = False
                self.page.update()

            self.page.overlay.append(dialog)
            dialog.open = True
            self.page.update()
            return

        checkboxes = []
        for tag in all_tags:
            cb = ft.Checkbox(
                label=tag.name,
                value=tag.id in movie_tag_ids,
                data=tag.id,
                fill_color={ft.ControlState.SELECTED: COLORS["primary"]},
                check_color=COLORS["background"],
            )
            checkboxes.append(cb)

        tags_column = ft.Column(controls=checkboxes, scroll=ft.ScrollMode.AUTO, spacing=0)

        def close_dialog(e=None):
            dialog.open = False
            self.page.update()

        def save_tags(e):
            selected_ids = [cb.data for cb in checkboxes if cb.value]

            async def do_save():
                try:
                    async with get_session() as session:
                        await set_movie_tags(session, movie.id, selected_ids)
                        updated_tags = await get_movie_tags(session, movie.id)
                        tag_names = [t.name for t in updated_tags]
                        if not is_shutting_down():
                            self.movie_list.update_movie_tags(movie.id, tag_names)
                except Exception:
                    pass

            self.page.run_task(do_save)
            close_dialog()

        dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Теги: {movie.title}", size=16, weight=ft.FontWeight.BOLD, color=COLORS["text_primary"]),
            content=ft.Container(
                content=tags_column,
                width=300,
            ),
            actions=[
                ft.TextButton("Отмена", on_click=close_dialog, style=ft.ButtonStyle(color=COLORS["text_secondary"])),
                ft.ElevatedButton("Сохранить", on_click=save_tags, bgcolor=COLORS["primary"], color=COLORS["background"]),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            bgcolor=COLORS["surface"],
        )

        self.page.overlay.append(dialog)
        dialog.open = True
        self.page.update()

    def _handle_similar_click(self, movie: Movie):
        """Handle find similar button click."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self._exit_stats_mode()
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

                    if not is_shutting_down():
                        try:
                            self.movie_list.update_rating(movie.id, user_rating)
                        except Exception:
                            pass
            except Exception:
                pass

            if not is_shutting_down():
                try:
                    self.page.update()
                except Exception:
                    pass  # Ignore if UI is destroyed

        self.page.run_task(do_save)

    async def _get_ratings_for_movies(self, movies: list[Movie]) -> dict[int, UserRating]:
        """Get user ratings for a list of movies (single batch query)."""
        if not movies:
            return {}
        async with get_session() as session:
            movie_ids = [m.id for m in movies]
            return await get_user_ratings_batch(session, movie_ids)

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
                    try:
                        self.movie_list.update_movie_data(movie)
                    except Exception:
                        pass  # Ignore if UI is being destroyed

                await self.search_service.fetch_missing_ratings(session, movies, on_movie_updated)
        except Exception:
            pass  # Silently ignore rating fetch errors
        finally:
            if not is_shutting_down():
                try:
                    # Turn off loading indicators when done
                    self.movie_list.set_ratings_loading(False)
                except Exception:
                    pass  # Ignore if UI is already destroyed

    def _handle_stats(self):
        """Handle statistics button click."""
        self._exit_ratings_mode()
        self._exit_wishlist_mode()
        self.is_stats_mode = True
        self.movie_list.on_fetch_more = None
        self.page.run_task(self._load_stats)

    async def _load_stats(self):
        """Load statistics data with current filters and display histogram."""
        try:
            async with get_session() as session:
                genres = self.search_bar.get_selected_genre_names()

                tag_names = None
                exclude_tag_names = None
                if self._selected_tag_ids or self._excluded_tag_ids:
                    all_tags = await get_all_tags(session)
                    if self._selected_tag_ids:
                        tag_names = [t.name for t in all_tags if t.id in self._selected_tag_ids]
                    if self._excluded_tag_ids:
                        exclude_tag_names = [t.name for t in all_tags if t.id in self._excluded_tag_ids]

                user_ratings = await get_all_user_ratings_filtered(
                    session,
                    sort_by="rating_desc",
                    genres=genres if genres else None,
                    tags=tag_names if tag_names else None,
                    exclude_tags=exclude_tag_names if exclude_tag_names else None,
                    rating_values=self._selected_rating_values if self._selected_rating_values else None,
                )

                counts = {v: 0 for v in range(1, 11)}
                for ur in user_ratings:
                    if 1 <= ur.rating <= 10:
                        counts[ur.rating] += 1
                total = len(user_ratings)

                if not is_shutting_down():
                    self._show_stats_content(counts, total)
        except Exception:
            pass

    def _show_stats_content(self, counts: dict[int, int], total: int):
        """Show statistics with rating histogram in the main content area."""
        RATING_COLORS = {
            1: "#F44336", 2: "#FF5722", 3: "#FF9800", 4: "#FFC107", 5: "#FFEB3B",
            6: "#CDDC39", 7: "#8BC34A", 8: "#4CAF50", 9: "#00BCD4", 10: "#2196F3",
        }

        max_count = max(counts.values()) if counts else 1
        if max_count == 0:
            max_count = 1

        # Use expand weights instead of pixel heights — Flet distributes
        # available vertical space automatically, so bars always fit.
        # Animation: inner container slides up via offset inside a clipped outer.
        animated_bars = []
        bars = []
        for value in range(1, 11):
            count = counts[value]

            # Spacer pushes bar down; bar fills proportional space
            spacer = ft.Container(expand=max_count - count if count > 0 else max_count)

            # Minimum expand so that small bars still have room for the label
            bar_expand = max(count, 4) if count > 0 else 0

            if count > 0:
                inner_bar = ft.Container(
                    content=ft.Column(
                        controls=[
                            ft.Text(
                                str(count),
                                size=13,
                                color=RATING_COLORS[value],
                                weight=ft.FontWeight.BOLD,
                                text_align=ft.TextAlign.CENTER,
                            ),
                            ft.Container(
                                bgcolor=RATING_COLORS[value],
                                border_radius=ft.border_radius.only(top_left=4, top_right=4),
                                expand=True,
                            ),
                        ],
                        spacing=4,
                        expand=True,
                        horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
                    ),
                    expand=True,
                    offset=ft.Offset(0, 1),
                    animate_offset=ft.Animation(600, ft.AnimationCurve.EASE_OUT),
                )
                bar = ft.Container(
                    expand=bar_expand,
                    clip_behavior=ft.ClipBehavior.HARD_EDGE,
                    content=inner_bar,
                )
                animated_bars.append(inner_bar)
            else:
                bar = ft.Container(
                    height=3,
                    bgcolor=RATING_COLORS[value],
                    border_radius=ft.border_radius.only(top_left=4, top_right=4),
                )

            bar_col = ft.Column(
                controls=[
                    spacer,
                    bar,
                    ft.Text(
                        str(value),
                        size=14,
                        color=COLORS["text_primary"],
                        weight=ft.FontWeight.BOLD,
                        text_align=ft.TextAlign.CENTER,
                    ),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
                spacing=2,
                expand=True,
            )
            bars.append(bar_col)

        histogram = ft.Row(
            controls=bars,
            vertical_alignment=ft.CrossAxisAlignment.END,
            spacing=4,
            expand=True,
        )

        if total > 0:
            avg = sum(v * counts[v] for v in range(1, 11)) / total
            summary_text = f"Средняя оценка: {avg:.2f}  •  Всего фильмов: {total}"
        else:
            summary_text = "Нет оценённых фильмов"

        content = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Container(
                        content=histogram,
                        expand=True,
                    ),
                    ft.Divider(color=COLORS["divider"]),
                    ft.Text(
                        summary_text,
                        size=14,
                        color=COLORS["text_secondary"],
                        text_align=ft.TextAlign.CENTER,
                    ),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=12,
                expand=True,
            ),
            padding=ft.padding.symmetric(vertical=10, horizontal=20),
            expand=True,
        )

        self.movie_list.set_custom_content(content)
        self.page.update()

        # Trigger grow animation — slide bars and labels up
        async def animate_bars():
            await asyncio.sleep(0.15)
            if is_shutting_down():
                return
            for ctrl in animated_bars:
                ctrl.offset = ft.Offset(0, 0)
            self.page.update()

        self.page.run_task(animate_bars)

    def _show_loading(self):
        """Show loading indicator."""
        self.movie_list.show_loading()
        self.page.update()

