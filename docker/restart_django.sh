echo "Stopping Django"
docker compose stop dj-dfm
echo "Starting Django"
docker compose up -d dj-dfm