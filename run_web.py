"""Flask application entry point."""

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from src.web import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
