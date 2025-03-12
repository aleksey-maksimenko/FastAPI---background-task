from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy import Column, Integer, String, Boolean, DateTime
import uuid
from datetime import datetime, timedelta

db_url = 'sqlite:///users.db'
engine = create_engine(db_url, echo=True)

class Base(DeclarativeBase):
    pass

# модель пользователя
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)

class UserSession(Base):
    __tablename__ = 'sessions'

    id = Column(String, primary_key=True)  # уникальный session_id
    user_id = Column(Integer, nullable=False)
    time_start = Column(DateTime, default=datetime.utcnow, nullable=False)

class UserDb:
    def __init__(self, engine_string=db_url):
        # двжиок для базы данных
        self.engine = create_engine(engine_string, echo=True)

    # добавить пользователя
    def insert_user(self, email: str, password: str):
        with Session(autoflush=False, bind=self.engine) as db:
            user = User(email=email, password=password)
            db.add(user)
            db.commit()

    # получения пользователя по ID
    def get_user_by_id(self, user_id: int):
        with Session(autoflush=False, bind=self.engine) as db:
            return db.query(User).filter_by(id=user_id).first()

    # доступ к записи пользователя по email
    def get_user_by_email(self, email: str):
        with Session(autoflush=False, bind=self.engine) as db:
            return db.query(User).filter_by(email=email).first()

    # добавить сессию
    def add_session(self, user_id: int):
        session_id = str(uuid.uuid4())  # генерируем уникальный session_id
        with Session(autoflush=False, bind=self.engine) as db:
            session = UserSession(id=session_id, user_id=user_id)
            db.add(session)
            db.commit()
            return session_id

    # удалить сессию по id
    def delete_session(self, session_id: str):
        with Session(autoflush=False, bind=self.engine) as db:
            session = db.query(UserSession).filter_by(id=session_id).first()
            if session:
                db.delete(session)
                db.commit()
                return True
            return False

    # удалить все сессии, открытые больше hours часов назад
    def clear_old_sessions(self, hours: int):
        with Session(autoflush=False, bind=self.engine) as db:
            threshold = datetime.utcnow() - timedelta(hours=hours)
            old_sessions = db.query(UserSession).filter(UserSession.time_start < threshold).all()
            for session in old_sessions:
                db.delete(session)
            db.commit()

    # получить сессию по id
    def get_session_by_id(self, session_id: str):
        with Session(autoflush=False, bind=self.engine) as db:
            return db.query(UserSession).filter_by(id=session_id).first()