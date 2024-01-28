import os
from datetime import datetime, timedelta
from jose import JWTError, jwt
from fastapi import HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer

ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_jwt_token(user: str):
    return jwt.encode(
        {"sub": user, "exp": datetime.utcnow() + timedelta(minutes=10)},
        os.environ["SECRET_KEY"],
        algorithm=ALGORITHM,
    )


def decode_jwt_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, os.environ["SECRET_KEY"], algorithms=ALGORITHM)
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
