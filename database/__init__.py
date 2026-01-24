from .models import Movie, UserRating, Genre, GenreRating, DirectorRating, ActorRating, RecommendationCache
from .db import (
    init_db, get_session, get_movie_by_kp_id, save_movie,
    get_user_rating, save_user_rating, delete_user_rating,
    get_all_user_ratings, get_all_user_ratings_filtered,
    get_rated_movies, search_local_movies,
    get_genre_rating, get_director_rating, get_actor_rating,
    get_cached_recommendations, save_cached_recommendations
)
from .genre_utils import normalize_genres
