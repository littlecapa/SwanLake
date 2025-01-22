#!/bin/sh

# Set the base directory for the Django application
APP_DIR="/app"
BASE_DIR="$APP_DIR/dj_dfm"
DJANGO_DIR="$BASE_DIR/app"
echo "APP_DIR: $APP_DIR BASE_DIR: $BASE_DIR DJANGO_DIR: $DJANGO_DIR"

rm -rf "$BASE_DIR"

# Navigate to the base directory
if [ ! -d "$BASE_DIR" ]; then
  echo "Base directory $BASE_DIR does not exist. Cloning repository..."
  cd "$APP_DIR" || { echo "Failed to navigate to $APP_DIR"; exit 1; }
  pwd
  ls -al
  git clone https://github.com/littlecapa/dj_dfm.git
  ls -al
fi

cd "$BASE_DIR" || { echo "Failed to navigate to $BASE_DIR"; exit 1; }

# Check if Git is already initialized in the directory
if [ -d .git ]; then
  echo "Repository exists. Pulling latest changes..."
  git reset --hard
  git clean -fd
  git pull origin main
  echo "Repository updated."
else
  echo "No repository found. Exit..."
  ls -al
  exit 1
fi

# Move to the Django app directory

if [ ! -d "$DJANGO_DIR" ]; then
  echo "Django app directory $APP_DIR does not exist."
  exit 1
fi
cd "$DJANGO_DIR" || { echo "Failed to navigate to $APP_DIR"; exit 1; }

# Apply migrations
echo "Applying migrations..."
python manage.py makemigrations
python manage.py migrate

# Start the Django server
echo "Starting Django app..."
exec "$@"
