import pandas as pd
from datetime import datetime
import os
import re
import logging

logger = logging.getLogger(__name__)

# Dataset configuration - change this to switch between datasets
DATASET_FOLDER = 'ml-latest-small'  # Change to 'ml-latest' or your dataset folder name

def format_timestamp(timestamp):
    """Convert Unix timestamp to readable date format."""
    try:
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f"Failed to format timestamp {timestamp}: {str(e)}")
        return timestamp

def format_genres(genres):
    """Convert pipe-delimited genres to comma-separated format."""
    if pd.isna(genres):
        return genres
    return genres.replace('|', ', ')

def format_title(title):
    """Format movie title by moving articles (The, A, An) from the end to the beginning."""
    if pd.isna(title) or not title:
        return title
    
    title = str(title)
    
    # Pattern: "Title, The" or "Title, A" or "Title, An"
    # Extract year if present (e.g., "Godfather, The (1972)")
    year_match = re.search(r'\s*\((\d{4})\)\s*$', title)
    year = year_match.group(1) if year_match else None
    title_without_year = re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
    
    # Check if title ends with ", The", ", A", or ", An"
    if title_without_year.endswith(', The'):
        formatted_title = 'The ' + title_without_year[:-5].strip()
    elif title_without_year.endswith(', A'):
        formatted_title = 'A ' + title_without_year[:-3].strip()
    elif title_without_year.endswith(', An'):
        formatted_title = 'An ' + title_without_year[:-4].strip()
    else:
        formatted_title = title_without_year
    
    # Add year back if it was present
    if year:
        formatted_title += f' ({year})'
    
    return formatted_title

def calculate_average_ratings(ratings_df):
    """Calculate average rating and count for each movie."""
    avg_ratings = ratings_df.groupby('movieId')['rating'].agg(['mean', 'count']).reset_index()
    avg_ratings.columns = ['movieId', 'avg_rating', 'rating_count']
    return avg_ratings

def merge_movies_with_ratings(movies_df, avg_ratings):
    """Merge movies with their average ratings."""
    movies_with_ratings = movies_df.merge(avg_ratings, on='movieId', how='left')
    movies_with_ratings['avg_rating'] = movies_with_ratings['avg_rating'].fillna(0)
    movies_with_ratings['rating_count'] = movies_with_ratings['rating_count'].fillna(0)
    return movies_with_ratings

def load_data():
    """Load and prepare all movie data."""
    try:
        # Use relative paths from the working directory (/app)
        movies_path = f'assets/{DATASET_FOLDER}/movies.csv'
        ratings_path = f'assets/{DATASET_FOLDER}/ratings.csv'
        
        logger.info(f"Loading movies from {movies_path}")
        movies_df = pd.read_csv(movies_path)
        logger.info(f"Loading ratings from {ratings_path}")
        ratings_df = pd.read_csv(ratings_path)
        
        # Format timestamps in the ratings dataframe
        ratings_df['timestamp'] = ratings_df['timestamp'].apply(format_timestamp)
        
        # Format genres to be comma-separated
        movies_df['genres'] = movies_df['genres'].apply(format_genres)
        
        # Format titles to move articles to the beginning
        movies_df['title'] = movies_df['title'].apply(format_title)
        
        # Calculate average ratings (this is the main operation that needs all ratings)
        avg_ratings = calculate_average_ratings(ratings_df)
        movies_with_ratings = merge_movies_with_ratings(movies_df, avg_ratings)
        
        logger.info(f"Data loaded successfully: {len(movies_df)} movies, {len(ratings_df)} ratings")
        return movies_df, ratings_df, movies_with_ratings
    except FileNotFoundError as e:
        logger.error(f"Data file not found: {str(e)}", exc_info=True)
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Data file is empty: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}", exc_info=True)
        raise

def get_ratings_over_time(movie_id, period='month'):
    """
    Get ratings over time data for a specific movie.
    
    Args:
        movie_id: The movie ID to get ratings for
        period: 'month' or 'year' - how to group the data
    
    Returns:
        Dictionary with:
        - periods: List of period labels (e.g., ['2018-01', '2018-02', ...])
        - avg_ratings: List of average ratings for each period
        - rating_counts: List of rating counts for each period
        - total_ratings: Total number of ratings
    """
    try:
        # Load raw ratings data (with Unix timestamps)
        ratings_path = f'assets/{DATASET_FOLDER}/ratings.csv'
        raw_ratings = pd.read_csv(ratings_path)
        
        # Filter for the specific movie
        movie_ratings = raw_ratings[raw_ratings['movieId'] == movie_id].copy()
        
        if len(movie_ratings) == 0:
            logger.debug(f"No ratings found for movie {movie_id}")
            return {
                'periods': [],
                'avg_ratings': [],
                'rating_counts': [],
                'total_ratings': 0
            }
        
        # Convert Unix timestamp to datetime
        movie_ratings['datetime'] = pd.to_datetime(movie_ratings['timestamp'], unit='s')
        
        # Group by period
        if period == 'month':
            movie_ratings['period'] = movie_ratings['datetime'].dt.to_period('M')
            movie_ratings['period_label'] = movie_ratings['datetime'].dt.strftime('%Y-%m')
        elif period == 'year':
            movie_ratings['period'] = movie_ratings['datetime'].dt.to_period('Y')
            movie_ratings['period_label'] = movie_ratings['datetime'].dt.strftime('%Y')
        else:
            raise ValueError("period must be 'month' or 'year'")
        
        # Group by period and calculate statistics
        grouped = movie_ratings.groupby('period').agg({
            'rating': ['mean', 'count'],
            'period_label': 'first'
        }).reset_index()
        
        grouped.columns = ['period', 'avg_rating', 'rating_count', 'period_label']
        grouped = grouped.sort_values('period')
        
        # Ensure period labels are strings and properly formatted
        if period == 'month':
            grouped['period_label'] = grouped['period'].astype(str).str[:7]  # Format as YYYY-MM
        else:
            grouped['period_label'] = grouped['period'].astype(str).str[:4]  # Format as YYYY
        
        logger.debug(f"Ratings over time for movie {movie_id}: {len(grouped)} periods")
        return {
            'periods': grouped['period_label'].tolist(),
            'avg_ratings': grouped['avg_rating'].round(2).tolist(),
            'rating_counts': grouped['rating_count'].astype(int).tolist(),
            'total_ratings': len(movie_ratings)
        }
    except FileNotFoundError as e:
        logger.error(f"Ratings file not found: {str(e)}", exc_info=True)
        return {
            'periods': [],
            'avg_ratings': [],
            'rating_counts': [],
            'total_ratings': 0
        }
    except Exception as e:
        logger.error(f"Error getting ratings over time for movie {movie_id}: {str(e)}", exc_info=True)
        return {
            'periods': [],
            'avg_ratings': [],
            'rating_counts': [],
            'total_ratings': 0
        }
