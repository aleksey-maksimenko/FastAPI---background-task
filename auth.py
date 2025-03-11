# auth.py
from fastapi import APIRouter, HTTPException
from models import UserCreate, UserResponse
from dbcontext import StudentsDb

router = APIRouter()
db = StudentsDb()


# регистрация пользователя
@router.post("/register", response_model=UserResponse)
async def register_user(user: UserCreate):
    existing_user = db.get_user_by_email(user.email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Почта уже зарегистрирована")
    # создание нового пользователя
    db.insert_user(user.email, user.password)
    existing_user = db.get_user_by_email(user.email)
    return {"id": existing_user.id, "email": user.email}


# авторизация пользователя
@router.post("/login")
async def login_user(user: UserCreate):
    db_user = db.get_user_by_email(user.email)
    if db_user is None or db_user.password != user.password:
        raise HTTPException(status_code=401, detail="Некорректные данные для входа")

    # активация сессии пользователя
    db.activate_user_session(db_user.id)
    return {"message": "Успешно вошли в систему", "user_id": db_user.id}


# завершение сессии пользователя
@router.post("/logout")
async def logout_user(user_id: int):
    db.deactivate_user_session(user_id)
    return {"message": "Сессия пользователя завершена"}


# проверка авторизации пользователя
def check_auth(user_id: int):
    db_user = db.get_user_by_id(user_id)
    if not db_user or not db_user.is_active:
        raise HTTPException(status_code=401, detail="Пользователь не авторизован")
