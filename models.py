from pydantic import BaseModel
from typing import Optional

# модуль с моделями Pydantic для работы с БД студентов

class StudentInsert(BaseModel):
    lastname: str
    firstname: str
    faculty: str
    course: str
    result: int

class StudentUpdate(BaseModel):
    lastname: Optional[str]
    firstname: Optional[str]
    faculty: Optional[str]
    course: Optional[str]
    result: Optional[int]

# модели для пользователей
class UserCreate(BaseModel):
    email: str
    password: str

class UserResponse(BaseModel):
    id: int
    email: str