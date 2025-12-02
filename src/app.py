from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
import os
from datetime import datetime
from collections import Counter
from data_management import load_data, get_ratings_over_time

app = Flask(__name__)

# Load data once at startup
movies_df, ratings_df, movies_with_ratings = load_data()

# Search logs file path
SEARCH_LOGS_FILE = 'search_logs.json'

def load_search_logs():
    """Load search logs from JSON file."""
    if os.path.exists(SEARCH_LOGS_FILE):
        try:
            with open(SEARCH_LOGS_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []

def save_search_log(search_term, genres):
    """Save a search log entry with search term, timestamp, and genres."""
    logs = load_search_logs()
    log_entry = {
        'search_term': search_term,
        'timestamp': datetime.now().isoformat(),
        'genres': genres
    }
    logs.append(log_entry)
    try:
        with open(SEARCH_LOGS_FILE, 'w') as f:
            json.dump(logs, f, indent=2)
    except IOError:
        pass  # Silently fail if file can't be written

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

@app.route('/')
def home():
    return render_template('home.html', 
                         total_movies=len(movies_df),
                         total_ratings=len(ratings_df))

@app.route('/movies')
def movies_page():
    limit = request.args.get('limit', default=50, type=int)
    # Filter movies with at least 10 ratings, then sort by average rating (highest first)
    popular_movies = movies_with_ratings[
        movies_with_ratings['rating_count'] >= 10
    ].sort_values('avg_rating', ascending=False).head(limit)
    movies = popular_movies.to_dict('records')
    # Ensure rating_count is an integer
    for movie in movies:
        movie['rating_count'] = int(movie['rating_count'])
    return render_template('movies.html', movies=movies, count=len(movies))

@app.route('/movie/<int:movie_id>')
def movie_page(movie_id):
    movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    
    if movie.empty:
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
    
    return render_template('movie.html', 
                         movie=movie_data, 
                         ratings_data=ratings_data,
                         recommendations=recs)

@app.route('/recommend/<int:movie_id>')
def recommend_page(movie_id):
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
    popular_movies = movies_with_ratings.sort_values('rating_count', ascending=False).head(limit)
    result = popular_movies[['movieId', 'title', 'genres', 'avg_rating', 'rating_count']].to_dict('records')
    return jsonify({"count": len(result), "movies": result})

@app.route('/api/movie/<int:movie_id>')
def api_movie(movie_id):
    movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    if movie.empty:
        return jsonify({"error": "Movie not found"}), 404
    movie_data = movie.iloc[0].to_dict()
    return jsonify({"movie": movie_data})

@app.route('/api/search')
def api_search():
    query = request.args.get('q', '').strip().lower()
    if not query or len(query) < 1:
        return jsonify({"count": 0, "movies": []})
    
    # Filter movies where title contains the query (case-insensitive)
    matching_movies = movies_with_ratings[
        movies_with_ratings['title'].str.lower().str.contains(query, na=False)
    ].head(20)  # Limit to 20 results for performance
    
    result = matching_movies[['movieId', 'title', 'genres', 'avg_rating', 'rating_count']].to_dict('records')
    
    # Extract and log genres from search results
    all_genres = set()
    for movie in result:
        if movie.get('genres') and pd.notna(movie['genres']):
            genres = [g.strip() for g in str(movie['genres']).split(',')]
            all_genres.update(genres)
    
    # Log the search if we found movies with genres
    if all_genres:
        save_search_log(query, list(all_genres))
    
    return jsonify({"count": len(result), "movies": result})

@app.route('/api/movie/<int:movie_id>/ratings-over-time')
def api_ratings_over_time(movie_id):
    """API endpoint for ratings over time data."""
    period = request.args.get('period', default='month', type=str)
    
    if period not in ['month', 'year']:
        return jsonify({"error": "period must be 'month' or 'year'"}), 400
    
    # Verify movie exists
    movie = movies_with_ratings[movies_with_ratings['movieId'] == movie_id]
    if movie.empty:
        return jsonify({"error": "Movie not found"}), 404
    
    ratings_data = get_ratings_over_time(movie_id, period=period)
    return jsonify(ratings_data)

@app.route('/genre-profile')
def genre_profile():
    """Display the user's genre profile based on search history."""
    profile_data = get_genre_profile_data()
    
    # Generate taste profile text
    taste_profile = generate_taste_profile(profile_data)
    
    return render_template('genre_profile.html',
                         profile_data=profile_data)

def generate_taste_profile(profile_data):
    """Generate a fun taste profile description."""
    if profile_data['total_searches'] == 0:
        return "Start searching for movies to discover your genre profile!"
    
    top_genres = profile_data['top_genres']
    if not top_genres:
        return "Your search history shows a diverse taste in movies!"
    
    top_genre = top_genres[0][0]
    top_count = top_genres[0][1]
    percentage = profile_data['genre_percentages'].get(top_genre, 0)
    
    descriptions = {
        'Action': 'You love high-energy films with thrilling sequences and explosive moments!',
        'Comedy': 'You have a great sense of humor and enjoy films that make you laugh!',
        'Drama': 'You appreciate deep storytelling and emotional narratives!',
        'Horror': 'You enjoy the thrill of suspense and spine-chilling experiences!',
        'Sci-Fi': 'You\'re fascinated by futuristic worlds and scientific possibilities!',
        'Romance': 'You enjoy heartwarming stories of love and connection!',
        'Thriller': 'You love edge-of-your-seat suspense and gripping plots!',
        'Adventure': 'You crave exciting journeys and epic quests!',
        'Animation': 'You appreciate the artistry and creativity of animated storytelling!',
        'Crime': 'You enjoy complex mysteries and criminal investigations!'
    }
    
    base_description = descriptions.get(top_genre, f'You have a strong preference for {top_genre} films!')
    
    if percentage >= 40:
        intensity = 'You\'re a true'
    elif percentage >= 25:
        intensity = 'You\'re a big'
    else:
        intensity = 'You\'re a'
    
    return f"{intensity} {top_genre} enthusiast! {base_description} Your searches show {top_genre} appears in {percentage}% of your movie interests."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
