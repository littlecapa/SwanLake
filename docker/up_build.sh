echo "Building docker containers"
echo "If you get mounting error, check all volumes are unmounted"
echo "Time Machine is a common culprit"

docker compose up --build -d --remove-orphans
