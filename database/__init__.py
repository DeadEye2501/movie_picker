from .models import Movie, UserRating, Genre, Director, Actor, Tag, Wishlist, RecommendationCache
from .db import (
    init_db, close_db, get_session, get_movie_by_kp_id, save_movie,
    get_user_rating, save_user_rating, delete_user_rating, update_entity_ratings_for_movie,
    get_all_user_ratings, get_all_user_ratings_filtered, get_user_ratings_batch,
    get_rated_movies, search_local_movies,
    get_genre_by_id, get_director_by_id, get_actor_by_id,
    get_or_create_director, get_or_create_actor,
    get_cached_recommendations, save_cached_recommendations,
    is_in_wishlist, add_to_wishlist, remove_from_wishlist, get_wishlist, get_wishlist_movie_ids,
    get_all_tags, create_tag, rename_tag, delete_tag, set_movie_tags, get_movie_tags,
)
from .genre_utils import normalize_genres_async
