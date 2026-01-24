import flet as ft


def get_dark_theme() -> ft.Theme:
    """Create a dark Material Design theme with lime accent."""
    return ft.Theme(
        color_scheme_seed=ft.Colors.LIME,
    )


COLORS = {
    "background": "#0D0D0D",
    "surface": "#1A1A1A",
    "surface_variant": "#252525",
    "primary": "#C6FF00",
    "primary_dark": "#9ECC00",
    "secondary": "#B2FF59",
    "text_primary": "#FFFFFF",
    "text_secondary": "#9E9E9E",
    "divider": "#333333",
    "star_filled": "#C6FF00",
    "star_empty": "#3D3D3D",
    "kp_rating": "#FF6600",
}
