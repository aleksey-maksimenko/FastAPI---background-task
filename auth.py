# auth.py
from fastapi import APIRouter, HTTPException
from models import UserCreate, UserResponse
from userdb import UserDb
from passlib.context import CryptContext


# настройка контекста для хеширования паролей
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], default="pbkdf2_sha256")

router = APIRouter()
user_db = UserDb()  # экземпляр UserDb для работы с таблицей пользователей

# регистрация пользователя
@router.post("/register", response_model=UserResponse)
async def register_user(user: UserCreate):
    existing_user = user_db.get_user_by_email(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Почта уже зарегистрирована")
    # создание нового пользователя с хешированным паролем
    hashed_password = pwd_context.hash(user.password)
    user_db.insert_user(user.email, hashed_password)
    existing_user = user_db.get_user_by_email(user.email)
    return {"id": existing_user.id, "email": user.email}


# авторизация пользователя
@router.post("/login")
async def login_user(user: UserCreate):
    db_user = user_db.get_user_by_email(user.email)
    if db_user is None or not pwd_context.verify(user.password, db_user.password):
        raise HTTPException(status_code=401, detail="Некорректные данные для входа")
    # создание новой сессии
    session_id = user_db.add_session(db_user.id)
    return {"message": "Успешно вошли в систему", "session_id": session_id}

# завершение сессии пользователя
@router.post("/logout")
async def logout_user(session_id: str):
    user_db.delete_session(session_id)
    return {"message": "Сессия пользователя завершена"}


# проверка авторизации пользователя
def check_auth(session_id: str):
    session = user_db.get_session_by_id(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Пользователь не авторизован")