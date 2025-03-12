from dbcontext import StudentsDb, Student, Base, engine
from models import StudentInsert, StudentUpdate, UserCreate, UserResponse
from fastapi import FastAPI, HTTPException
from auth import router as auth_router
from auth import check_auth
import userdb
from fastapi import BackgroundTasks
from sqlalchemy.orm import Session
from typing import List
from redis_cache import RedisCache
import json

app = FastAPI()
app.include_router(auth_router, prefix="/auth", tags=["auth"])
db = StudentsDb()
redis_cache = RedisCache()

@app.on_event("startup")
async def startup():
    Base.metadata.create_all(bind=engine)
    userdb.Base.metadata.create_all(bind=userdb.engine)
    await redis_cache.init() # инициализация Redis

@app.on_event("shutdown")
async def shutdown():
    await redis_cache.close() # закрытие Redis-соединения

# эндпоинт для добавления нового студента
@app.post("/students/")
async def create_student(student: StudentInsert, session_id: str):
    check_auth(session_id)  # проверка авторизации
    new_student = Student(
        lastname=student.lastname,
        firstname=student.firstname,
        faculty=student.faculty,
        course=student.course,
        result=student.result
    )
    db.insert_student(new_student)
    return {"message": "Студент добавлен успешно"}

# эндпоинт для получения всех студентов
@app.get("/students/")
async def read_students(session_id: str):
    '''
    check_auth(session_id)  # проверка авторизации
    students = db.select_students()
    return [{"id": s.id, "lastname": s.lastname, "firstname": s.firstname, "faculty": s.faculty, "course": s.course, "result": s.result} for s in students]
    '''
    # пробуем получить список студентов из кеша
    cache_key = "students_list"
    cached_students = await redis_cache.get(cache_key)
    if cached_students:
        return json.loads(cached_students)

    # если данных нет в кеше, запрашиваем их из базы данных
    students = db.select_students()
    # список словарей
    students_data = [
        {"id": s.id, "lastname": s.lastname, "firstname": s.firstname, "faculty": s.faculty, "course": s.course,
         "result": s.result}
        for s in students
    ]
    # сохраняем данные в кеш с TTL 3600 секунд * 12 (12 часов)
    await redis_cache.set(cache_key, json.dumps(students_data), ttl=3600 * 12)
    return students_data

# эндпоинт для обновления данных студента
@app.patch("/students/{student_id}")
async def update_student(student_id: int, student: StudentUpdate, session_id: str):
    check_auth(session_id)  # проверка авторизации
    if db.update_student(student_id, student.lastname, student.firstname, student.faculty, student.course, student.result):
        return {"message": "Данные студента обновлены успешно"}
    else:
        raise HTTPException(status_code=404, detail="Студент не найден")

# эндпоинт для удаления студента по id
@app.delete("/students/{student_id}")
async def delete_student(student_id: int, session_id: str):
    check_auth(session_id)  # проверка авторизации
    if db.delete_student(student_id):
        return {"message": "Студент удален успешно"}
    else:
        raise HTTPException(status_code=404, detail="Студент не найден")

# функция для выполнения загрузки данных о студентах из файла
def fill_database_from_csv(csv_path: str):
    try:
        db.insert_from_csv(csv_path)
    except Exception as e:
        print(f"Ошибка при заполнении базы данных: {e}")

# функция для удаления списка студентов
def delete_students_by_ids(student_ids: list[int]):
    try:
        with Session(autoflush=False, bind=db.engine) as session:
            students_to_delete = session.query(Student).filter(Student.id.in_(student_ids)).all()
            for student in students_to_delete:
                session.delete(student)
            session.commit()
    except Exception as e:
        print(f"Ошибка при удалении студентов: {e}")

# эндпоинт для фонового чтения базы данных из csv-файла
@app.post("/fill_database/")
async def fill_database(csv_path: str, session_id: str, back_tasks: BackgroundTasks):
    check_auth(session_id)  # проверка авторизации
    back_tasks.add_task(fill_database_from_csv, csv_path)
    return {"message": "Заполнение базы данных запущено в фоновом режиме"}

# эндпоинт, который запускает фоновую задачу по удалению студентов
@app.post("/delete_students/")
async def delete_students(student_ids: List[int], session_id: str, background_tasks: BackgroundTasks):
    check_auth(session_id)  # проверка авторизации
    background_tasks.add_task(delete_students_by_ids, student_ids)
    return {"message": "Удаление студентов запущено в фоновом режиме"}