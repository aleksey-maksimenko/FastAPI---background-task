from dbcontext import StudentsDb, Student, Base, engine
from models import StudentInsert, StudentUpdate, UserCreate, UserResponse
from fastapi import FastAPI, HTTPException
from auth import router as auth_router
from auth import check_auth

app = FastAPI()
app.include_router(auth_router, prefix="/auth", tags=["auth"])
db = StudentsDb()

@app.on_event("startup")
async def startup():
    Base.metadata.create_all(bind=engine)

# эндпоинт для добавления нового студента
@app.post("/students/")
async def create_student(student: StudentInsert, user_id: int):
    check_auth(user_id)  # проверка авторизации
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
async def read_students(user_id: int):
    check_auth(user_id)  # проверка авторизации
    students = db.select_students()
    return [{"id": s.id, "lastname": s.lastname, "firstname": s.firstname, "faculty": s.faculty, "course": s.course, "result": s.result} for s in students]


# эндпоинт для обновления данных студента
@app.patch("/students/{student_id}")
async def update_student(student_id: int, student: StudentUpdate, user_id: int):
    check_auth(user_id)  # проверка авторизации
    if db.update_student(student_id, student.lastname, student.firstname, student.faculty, student.course, student.result):
        return {"message": "Данные студента обновлены успешно"}
    else:
        raise HTTPException(status_code=404, detail="Студент не найден")


# эндпоинт для удаления студента по id
@app.delete("/students/{student_id}")
async def delete_student(student_id: int, user_id: int):
    check_auth(user_id)  # проверка авторизации
    if db.delete_student(student_id):
        return {"message": "Студент удален успешно"}
    else:
        raise HTTPException(status_code=404, detail="Студент не найден")
