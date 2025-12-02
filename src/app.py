from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
import os
import re
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from collections import Counter
from data_management import load_data, get_ratings_over_time

app = Flask(__name__)

# Load configuration from environment variables
SEARCH_LOGS_FILE = os.getenv('SEARCH_LOGS_FILE', 'search_logs.json')
DEBUG_MODE = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
MAX_SEARCH_LENGTH = int(os.getenv('MAX_SEARCH_LENGTH', '100'))
MAX_LIMIT = int(os.getenv('MAX_LIMIT', '1000'))
LOG_FILE = os.getenv('LOG_FILE', 'app.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Configure logging
def setup_logging():
    """Configure application logging with file and console handlers."""
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler with rotation (max 10MB, keep 5 backups)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    file_handler.setFormatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logging
logger = setup_logging()
app.logger = logger

# Load data once at startup
try:
    logger.info("Loading movie data...")
    movies_df, ratings_df, movies_with_ratings = load_data()
    logger.info(f"Successfully loaded {len(movies_df)} movies and {len(ratings_df)} ratings")
except Exception as e:
    logger.error(f"Failed to load data: {str(e)}", exc_info=True)
    raise

def sanitize_input(text, max_length=MAX_SEARCH_LENGTH):
    """Sanitize and validate user input."""
    if not text or not isinstance(text, str):
        return None
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Enforce length limit
    if len(text) > max_length:
        text = text[:max_length]
    
    # Remove any null bytes and control characters
    text = text.replace('\x00', '')
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    
    # Return None if empty after sanitization
    if not text:
        return None
    
    return text

def hash_search_term(search_term):
    """Hash search term for privacy while preserving genre data."""
    if not search_term:
        return None
    # Use SHA256 for one-way hashing
    return hashlib.sha256(search_term.encode('utf-8')).hexdigest()[:16]  # Use first 16 chars for shorter hashes

def load_search_logs():
    """Load search logs from JSON file."""
    if os.path.exists(SEARCH_LOGS_FILE):
        try:
            with open(SEARCH_LOGS_FILE, 'r') as f:
                logs = json.load(f)
                logger.debug(f"Loaded {len(logs)} search log entries")
                return logs
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse search logs JSON: {str(e)}", exc_info=True)
            return []
        except IOError as e:
            logger.error(f"Failed to read search logs file: {str(e)}", exc_info=True)
            return []
    return []

def save_search_log(search_term, genres):
    """Save a search log entry with hashed search term, timestamp, and genres."""
    # Sanitize search term before hashing
    sanitized_term = sanitize_input(search_term)
    if not sanitized_term:
        return
    
    # Hash the search term for privacy
    hashed_term = hash_search_term(sanitized_term)
    
    logs = load_search_logs()
    log_entry = {
        'search_term_hash': hashed_term,  # Store hash instead of plain text
        'timestamp': datetime.now().isoformat(),
        'genres': genres
    }
    logs.append(log_entry)
    try:
        with open(SEARCH_LOGS_FILE, 'w') as f:
            json.dump(logs, f, indent=2)
        logger.debug(f"Saved search log entry (hash: {hashed_term[:8]}...)")
    except IOError as e:
        logger.error(f"Failed to write search log: {str(e)}", exc_info=True)

def aggregate_genres():
    """Aggregate genre counts from search logs."""
    logs = load_search_logs()
    genre_counter = Counter()
    
    for log in logs:
        if log.get('genres'):
            for genre in log['genres']:
                genre_counter[genre] += 1
    
    return dict(genre_counter)

def get_genre_profile_data():
    """Get aggregated genre profile data for display."""
    logs = load_search_logs()
    genre_counts = aggregate_genres()
    total_searches = len(logs)
    
    # Calculate percentages
    genre_percentages = {}
    if total_searches > 0:
        for genre, count in genre_counts.items():
            genre_percentages[genre] = round((count / total_searches) * 100, 1)
    
    # Get top 3 genres
    top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return {
        'total_searches': total_searches,
        'genre_counts': genre_counts,
        'genre_percentages': genre_percentages,
        'top_genres': top_genres
    }

@app.errorhandler(404)
def not_found_error(error):
    logger.warning(f"404 error: {request.url}")
    return render_template('error.html', error="Page not found"), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"500 error: {str(error)}", exc_info=True)
    return render_template('error.html', error="An internal error occurred"), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    return render_template('error.html', error="An unexpected error occurred"), 500

@app.route('/')
def home():
    try:
        return render_template('home.html', 
                             total_movies=len(movies_df),
                             total_ratings=len(ratings_df))
    except Exception as e:
        logger.error(f"Error in home route: {str(e)}", exc_info=True)
        raise

@app.route('/movies')
def movies_page():
    try:
        limit = request.args.get('limit', default=50, type=int)
        # Validate and limit the limit parameter
        limit = max(1, min(limit, MAX_LIMIT))  # Ensure between 1 and MAX_LIMIT
        
        # Filter movies with at least 10 ratings, then sort by average rating (highest first)
        popular_movies = movies_with_ratings[
            movies_with_ratings['rating_count'] >= 10
        ].sort_values('avg_rating', ascending=False).head(limit)
        movies = popular_movies.to_dict('records')
        # Ensure rating_count is an integer
        for movie in movies:
            movie['rating_count'] = int(movie['rating_count'])
        logger.debug(f"Movies page: returning {len(movies)} movies")
        return render_template('movies.html', movies=movies, count=len(movies))
    except Exception as e:
        logger.error(f"Error in movies_page route: {str(e)}", exc_info=True)
        raise

@app.route('/movie/<int:movie_id>')
def movie_page(movie_id):
    try:
        # Validate movie_id is positive
        if movie_id <= 0:
            logger.warning(f"Invalid movie_id requested: {movie_id}")
            return render_template('error.html', error="Invalid movie ID"), 400
        
        movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
        
        if movie.empty:
            logger.warning(f"Movie not found: {movie_id}")
            return render_template('error.html', error="Movie not found"), 404
        
        movie_data = movie.iloc[0].to_dict()
        # Ensure rating_count is an integer
        movie_data['rating_count'] = int(movie_data['rating_count'])
        
        # Get ratings over time data for the chart
        ratings_data = get_ratings_over_time(movie_id, period='year')
        
        # Get recommendations (same logic as recommend_page)
        source_genres = movie_data['genres']
        if pd.isna(source_genres) or source_genres == '':
            genre_list = []
        else:
            genre_list = [g.strip() for g in source_genres.split(',')]
        
        def genre_match_score(genres):
            if pd.isna(genres) or genres == '':
                return 0
            movie_genres = set([g.strip() for g in genres.split(',')])
            source_genres_set = set(genre_list)
            return len(movie_genres.intersection(source_genres_set))
        
        movies_with_ratings['match_score'] = movies_with_ratings['genres'].apply(genre_match_score)
        
        recommendations = movies_with_ratings[
            (movies_with_ratings['movieId'] != movie_id) & 
            (movies_with_ratings['rating_count'] >= 10) &
            (movies_with_ratings['match_score'] > 0)
        ].sort_values(['match_score', 'avg_rating'], ascending=[False, False]).head(6)
        
        recs = recommendations.to_dict('records')
        # Ensure rating_count is an integer for recommendations
        for rec in recs:
            rec['rating_count'] = int(rec['rating_count'])
        
        logger.debug(f"Movie page: {movie_id} - {movie_data['title']}")
        return render_template('movie.html', 
                             movie=movie_data, 
                             ratings_data=ratings_data,
                             recommendations=recs)
    except Exception as e:
        logger.error(f"Error in movie_page route for movie_id {movie_id}: {str(e)}", exc_info=True)
        raise

@app.route('/recommend/<int:movie_id>')
def recommend_page(movie_id):
    # Validate movie_id is positive
    if movie_id <= 0:
        return render_template('error.html', error="Invalid movie ID"), 400
    
    source_movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    
    if source_movie.empty:
        return render_template('error.html', error="Movie not found"), 404
    
    source_genres = source_movie.iloc[0]['genres']
    source_title = source_movie.iloc[0]['title']
    if pd.isna(source_genres) or source_genres == '':
        genre_list = []
    else:
        genre_list = [g.strip() for g in source_genres.split(',')]
    
    def genre_match_score(genres):
        if pd.isna(genres) or genres == '':
            return 0
        movie_genres = set([g.strip() for g in genres.split(',')])
        source_genres_set = set(genre_list)
        return len(movie_genres.intersection(source_genres_set))
    
    movies_with_ratings['match_score'] = movies_with_ratings['genres'].apply(genre_match_score)
    
    recommendations = movies_with_ratings[
        (movies_with_ratings['movieId'] != movie_id) & 
        (movies_with_ratings['rating_count'] >= 10) &
        (movies_with_ratings['match_score'] > 0)
    ].sort_values(['match_score', 'avg_rating'], ascending=[False, False]).head(6)
    
    recs = recommendations.to_dict('records')
    # Ensure rating_count is an integer
    for rec in recs:
        rec['rating_count'] = int(rec['rating_count'])
    
    return render_template('recommendations.html',
                         source_movie={'id': movie_id, 'title': source_title, 'genres': source_genres},
                         recommendations=recs)

@app.route('/movie/<int:movie_id>/ratings-over-time')
def ratings_over_time_page(movie_id):
    # Validate movie_id is positive
    if movie_id <= 0:
        return render_template('error.html', error="Invalid movie ID"), 400
    
    movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    
    if movie.empty:
        return render_template('error.html', error="Movie not found"), 404
    
    movie_data = movie.iloc[0].to_dict()
    
    # Get ratings over time data (default to monthly)
    ratings_data = get_ratings_over_time(movie_id, period='month')
    
    return render_template('ratings_over_time.html',
                         movie=movie_data,
                         ratings_data=ratings_data)

# API endpoints (keep for backwards compatibility)
@app.route('/api/movies')
def api_movies():
    limit = request.args.get('limit', default=50, type=int)
    # Validate and limit the limit parameter
    limit = max(1, min(limit, MAX_LIMIT))  # Ensure between 1 and MAX_LIMIT
    
    popular_movies = movies_with_ratings.sort_values('rating_count', ascending=False).head(limit)
    result = popular_movies[['movieId', 'title', 'genres', 'avg_rating', 'rating_count']].to_dict('records')
    return jsonify({"count": len(result), "movies": result})

@app.route('/api/movie/<int:movie_id>')
def api_movie(movie_id):
    # Validate movie_id is positive
    if movie_id <= 0:
        return jsonify({"error": "Invalid movie ID"}), 400
    
    movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    if movie.empty:
        return jsonify({"error": "Movie not found"}), 404
    movie_data = movie.iloc[0].to_dict()
    return jsonify({"movie": movie_data})

@app.route('/api/search')
def api_search():
    try:
        query = request.args.get('q', '')
        
        # Sanitize and validate input
        query = sanitize_input(query, MAX_SEARCH_LENGTH)
        if not query:
            return jsonify({"count": 0, "movies": []})
        
        # Convert to lowercase for case-insensitive search
        query_lower = query.lower()
        
        # Escape special regex characters to prevent injection
        query_escaped = re.escape(query_lower)
        
        # Filter movies where title contains the query (case-insensitive)
        try:
            matching_movies = movies_with_ratings[
                movies_with_ratings['title'].str.lower().str.contains(query_escaped, na=False, regex=True)
            ].head(20)  # Limit to 20 results for performance
        except Exception as e:
            logger.warning(f"Regex search failed for query '{query}', using fallback: {str(e)}")
            # Fallback to simple contains if regex fails
            matching_movies = movies_with_ratings[
                movies_with_ratings['title'].str.lower().str.contains(query_lower, na=False, regex=False)
            ].head(20)
        
        result = matching_movies[['movieId', 'title', 'genres', 'avg_rating', 'rating_count']].to_dict('records')
        
        # Extract and log genres from search results
        all_genres = set()
        for movie in result:
            if movie.get('genres') and pd.notna(movie['genres']):
                genres = [g.strip() for g in str(movie['genres']).split(',')]
                # Sanitize genre names
                sanitized_genres = [sanitize_input(g, 50) for g in genres if sanitize_input(g, 50)]
                all_genres.update(sanitized_genres)
        
        # Log the search if we found movies with genres
        if all_genres:
            save_search_log(query, list(all_genres))
        
        logger.debug(f"Search query '{query}' returned {len(result)} results")
        return jsonify({"count": len(result), "movies": result})
    except Exception as e:
        logger.error(f"Error in api_search route: {str(e)}", exc_info=True)
        return jsonify({"error": "Search failed", "count": 0, "movies": []}), 500

@app.route('/api/movie/<int:movie_id>/ratings-over-time')
def api_ratings_over_time(movie_id):
    """API endpoint for ratings over time data."""
    try:
        period = request.args.get('period', default='month', type=str)
        
        # Validate period parameter - whitelist approach
        if period not in ['month', 'year']:
            logger.warning(f"Invalid period parameter: {period}")
            return jsonify({"error": "period must be 'month' or 'year'"}), 400
        
        # Validate movie_id is positive
        if movie_id <= 0:
            logger.warning(f"Invalid movie_id in ratings-over-time: {movie_id}")
            return jsonify({"error": "Invalid movie ID"}), 400
        
        # Verify movie exists
        movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
        if movie.empty:
            logger.warning(f"Movie not found for ratings-over-time: {movie_id}")
            return jsonify({"error": "Movie not found"}), 404
        
        ratings_data = get_ratings_over_time(movie_id, period=period)
        logger.debug(f"Ratings over time for movie {movie_id}, period {period}: {ratings_data['total_ratings']} ratings")
        return jsonify(ratings_data)
    except Exception as e:
        logger.error(f"Error in api_ratings_over_time route for movie_id {movie_id}: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to retrieve ratings data"}), 500

@app.route('/genre-profile')
def genre_profile():
    """Display the user's genre profile based on search history."""
    profile_data = get_genre_profile_data()
    
    return render_template('genre_profile.html',
                         profile_data=profile_data)

if __name__ == '__main__':
    # Use PORT environment variable (set by Azure) or default to 5000
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=DEBUG_MODE)
