#virtualenv com Pipenv

uvicorn main:app --reload &
xdg-open http://127.0.0.1:8000/docs &
