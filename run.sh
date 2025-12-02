#!/bin/bash
docker build -t movielens-api:latest .

docker stop movielens-api 2>/dev/null
docker rm movielens-api 2>/dev/null

# Run container
docker run -d \
  --name movielens-api \
  -p 5000:5000 \
  movielens-api:latest


sleep 2

echo "API running at http://localhost:5000"
echo "Test: curl http://localhost:5000/movie/1"
