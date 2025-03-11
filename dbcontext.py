from sqlalchemy import create_engine, func
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy import Column, Integer, String, Boolean

import pandas as pd

db_url = 'sqlite:///students.db'
engine = create_engine(db_url, echo=True)

class Base(DeclarativeBase):
    pass

class Student(Base):
    __tablename__ = 'students'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    lastname = Column(String) # фамилия
    firstname = Column(String) # имя
    faculty = Column(String) # факультет
    course = Column(String) # курс
    result = Column(Integer) # баллы

# модель пользователя
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)
    is_active = Column(Boolean, default=False)  # флаг активности сессии

# класс для работы с базой данных
class StudentsDb:

    def __init__(self, engine_string=db_url):
        # двжиок для базы данных
        self.engine = create_engine(engine_string, echo=True)

    # метод добавления студента
    def insert_student(self, student : Student):
        with Session(autoflush=False, bind=self.engine) as db:
            db.add(student)
            db.commit()

    # метод получения всех записей студентов
    def select_students(self):
        with Session(autoflush=False, bind=self.engine) as db:
            students = db.query(Student).all()
            return students

    # метод заполнения таблицы студентов из csv-файла
    def insert_from_csv(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
            students = []
            for index, row in df.iterrows():
                student = Student(
                    lastname=row['Фамилия'],
                    firstname=row['Имя'],
                    faculty=row['Факультет'],
                    course=row['Курс'],
                    result=row['Оценка']
                )
                students.append(student)
            with Session(autoflush=False, bind=self.engine) as db:
                db.add_all(students)
                db.commit()
        except FileNotFoundError:
            print(f'Файл {csv_path} не найден')
        except pd.errors.ParserError as e:
            print(f'Ошибка чтения файла {csv_path}: {e}')
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод получения списка студентов по названию факультета
    def select_students_by_fac(self, faculty):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                students = db.query(Student).filter_by(faculty=faculty).all()
                return students
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод получения списка уникальных курсов
    def unique_courses(self):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                unique_courses = db.query(Student.course).distinct().all()
                return [course[0] for course in unique_courses]
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод получения среднего балла по факультету
    def mean_result(self, faculty):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                mean_result = db.query(func.avg(Student.result)).filter_by(faculty=faculty).scalar()
                return mean_result
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод получения списка студентов по выбранному курсу с оценкой ниже 30 баллов
    def select_students_by_course_lowest(self, course_name):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                students = db.query(Student).filter_by(course=course_name).filter(Student.result < 30).all()
                return students
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод обновления данных студента
    def update_student(self, student_id, lastname=None, firstname=None, faculty=None, course=None, result=None):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                student = db.query(Student).filter_by(id=student_id).first()
                if student:
                    if lastname: # обновляем
                        student.lastname = lastname
                    if firstname:
                        student.firstname = firstname
                    if faculty:
                        student.faculty = faculty
                    if course:
                        student.course = course
                    if result:
                        student.result = result
                    db.commit()
                    return True
                else:
                    return False
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # метод удаления студента по id
    def delete_student(self, student_id):
        try:
            with Session(autoflush=False, bind=self.engine) as db:
                student = db.query(Student).filter_by(id=student_id).first()
                if student:
                    db.delete(student)
                    db.commit()
                    return True
                else:
                    return False
        except Exception as e:
            print(f'Произошла ошибка: {e}')

    # добавить пользователя
    def insert_user(self, email: str, password: str):
        with Session(autoflush=False, bind=self.engine) as db:
            user = User(email=email, password=password)
            db.add(user)
            db.commit()

    # активация сессии пользователя (при авторизации)
    def activate_user_session(self, user_id: int):
        with Session(autoflush=False, bind=self.engine) as db:
            user = db.query(User).filter_by(id=user_id).first()
            if user:
                user.is_active = True
                db.commit()

    # деактивация сессии пользователя (при выходе)
    def deactivate_user_session(self, user_id: int):
        with Session(autoflush=False, bind=self.engine) as db:
            user = db.query(User).filter_by(id=user_id).first()
            if user:
                user.is_active = False
                db.commit()
    # получения пользователя по ID
    def get_user_by_id(self, user_id: int):
        with Session(autoflush=False, bind=self.engine) as db:
            return db.query(User).filter_by(id=user_id).first()

    # доступ к записи пользователя по email
    def get_user_by_email(self, email: str):
        with Session(autoflush=False, bind=self.engine) as db:
            return db.query(User).filter_by(email=email).first()