import requests
import time

BASE_URL = "http://localhost:5000"

def test_home():
    """Test home page endpoint returns 200"""
    response = requests.get(f"{BASE_URL}/")
    assert response.status_code == 200
    assert "MovieLens Explorer" in response.text or "movie" in response.text.lower()
    print("✓ Home page working")

def test_movies_list():
    """Test movies list page returns 200"""
    response = requests.get(f"{BASE_URL}/movies")
    assert response.status_code == 200
    print("✓ Movies list page working")

def test_movie_detail():
    """Test movie detail page returns 200"""
    response = requests.get(f"{BASE_URL}/movie/1")
    assert response.status_code == 200
    print("✓ Movie detail page working")

def test_genre_profile():
    """Test genre profile page returns 200"""
    response = requests.get(f"{BASE_URL}/genre-profile")
    assert response.status_code == 200
    print("✓ Genre profile page working")

def test_api_movies():
    """Test JSON API endpoint for movies list"""
    response = requests.get(f"{BASE_URL}/api/movies")
    assert response.status_code == 200
    data = response.json()
    assert "movies" in data or "count" in data
    print("✓ JSON API movies endpoint working")

def test_api_movie():
    """Test JSON API endpoint for single movie"""
    response = requests.get(f"{BASE_URL}/api/movie/1")
    assert response.status_code == 200
    data = response.json()
    assert "movie" in data
    print("✓ JSON API movie endpoint working")

if __name__ == "__main__":
    print("Running smoke tests...")
    print("Make sure the app is running on localhost:5000")
    time.sleep(1)
    
    try:
        test_home()
        test_movies_list()
        test_movie_detail()
        test_genre_profile()
        test_api_movies()
        test_api_movie()
        print("\n✓ All tests passed!")
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
