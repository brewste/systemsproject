import requests
import time

BASE_URL = "http://localhost:5000"

def test_home():
    """Test home endpoint returns 200"""
    response = requests.get(f"{BASE_URL}/")
    assert response.status_code == 200
    print("Home endpoint working")

def test_movies():
    """Test movies endpoint returns data"""
    response = requests.get(f"{BASE_URL}/movies")
    assert response.status_code == 200
    print("✓ Movies endpoint working")

def test_movie_detail():
    """Test movie detail endpoint"""
    response = requests.get(f"{BASE_URL}/movie/1")
    assert response.status_code == 200
    print("✓ Movie detail endpoint working")

def test_recommendations():
    """Test recommendations endpoint"""
    response = requests.get(f"{BASE_URL}/recommend/1")
    assert response.status_code == 200
    print("✓ Recommendations endpoint working")

def test_api_movies():
    """Test JSON API endpoint"""
    response = requests.get(f"{BASE_URL}/api/movies")
    assert response.status_code == 200
    data = response.json()
    assert "movies" in data
    print("✓ API endpoint working")

if __name__ == "__main__":
    print("Running smoke tests...")
    print("Make sure the app is running on localhost:5000")
    time.sleep(1)
    
    try:
        test_home()
        test_movies()
        test_movie_detail()
        test_recommendations()
        test_api_movies()
        print("\n✓ All tests passed!")
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
