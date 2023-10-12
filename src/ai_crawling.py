import bs4.element
import requests
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, ForeignKey, Text
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy import Column, Integer, CHAR, ARRAY, DateTime, String, TIMESTAMP
import sqlalchemy
import configuration
import boto3

AI_BASE_URL = "http://aix.ssu.ac.kr/"

Base = declarative_base()

db_url = sqlalchemy.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=configuration.db_user_name,
    password=configuration.db_pw,
    host=configuration.db_host,
    database=configuration.db_name,
)

engine = create_engine(db_url)
session_maker = sessionmaker(autoflush=False, autocommit=False, bind=engine)
metadata_obj = MetaData()


# main.department 구조
class Department(Base):
    __tablename__ = "department"
    __table_args__ = {"schema": "main"}
    id = Column(Integer, primary_key=True)
    name = Column(String(32), nullable=False)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


def save_to_s3(file_name):
    s3 = boto3.resource("s3")
    bucket_name = configuration.bucket_name
    bucket = s3.Bucket(bucket_name)

    local_file = file_name
    obj_file = file_name

    bucket.upload_file(local_file, obj_file)


class AiNotification(Base):
    __tablename__ = "notice"
    __table_args__ = {"schema": "notice", "extend_existing": True}
    id = Column(Integer, primary_key=True)
    title = Column(CHAR(1024))
    department_id = Column(Integer)
    content = Column(CHAR(2048))
    category = Column(CHAR(32))
    image_url = Column(ARRAY(CHAR(2048)))
    file_url = Column(ARRAY(CHAR(2048)))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    views = Column(Integer)

    def __init__(self, row: bs4.element.Tag):
        childrens = row.find_all("td")

        if childrens:
            href = childrens[0].find("a")["href"]
            self.__link = AI_BASE_URL + href
        else:
            return

        req = requests.get(self.__link)
        soup = BeautifulSoup(req.text, "lxml")
        contents = soup.find("table", class_="table").find_all("p")

        # 제목
        self.title = childrens[0].text.strip()

        # 내용
        self.content = ""
        memo = ""
        for content in contents:
            memo += content.text

        if len(memo.encode("utf-8")) > 2048:
            file_name = "AI" + str(datetime.now().strftime("%Y%m%d%H%M%S")) + ".txt"
            with open(file_name, "w", encoding="utf-8") as file:
                file.write(memo)

            save_to_s3(file_name)

        # 카테고리
        self.category = "AI융합학부"

        # 이미지
        self.image_url = []

        # 파일
        file_link = []

        contents = soup.find("table", class_="table").find_all("li")
        for content in contents:
            link_tag = content.find("a")

            if link_tag is None:
                pass
            else:
                file_link.append(AI_BASE_URL + link_tag["href"])

        self.file_url = file_link

        # 생성 시각
        created_date = list(map(int, childrens[2].text.split(".")))
        self.created_at = date(created_date[0], created_date[1], created_date[2])

        # 업데이트 시각
        self.updated_at = datetime.now().strftime("%Y-%m-%d")

        # 조회수
        self.views = childrens[3].text.strip()

        # 학과
        with engine.connect() as connect:
            department_table = Table(
                "department", metadata_obj, schema="main", autoload_with=engine
            )

            query = department_table.select().where(department_table.c.name == "AI융합학부")
            results = connect.execute(query)

            for result in results:
                self.department_id = result.id

    def __str__(self):
        return (
            "title: {0}\n"
            "content: {1}\n"
            "image_url: {2}\n"
            "file_url: {3}\n"
            "department_id: {4}".format(
                self.title,
                self.content,
                self.image_url,
                self.file_url,
                self.department_id,
            )
        )


def ai_department_crawling(value):
    page = 1
    url = AI_BASE_URL + "notice.html?searchKey=ai"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "lxml")
    content = soup.find("table", class_="table")
    rows = content.find_all("tr")
    results = []

    for row in rows[1:]:
        results.append(AiNotification(row))

    with session_maker() as session:
        for result in results:
            # print(result)  # db 삽입 내용 확인 출력문

            # department.name에 AI융합학부가 없을 시 실행
            """
            if result.department_id is None:
            # AI융합학부 column 추가 로직
            new_department = Department(
                id=4,
                name="AI융합학부",
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            session.add(new_department)
            session.commit()
            session.close()

            result.department_id = new_department.id
            """

            session.add(result)

        session.commit()


def departments_crawling(value):
    ai_department_crawling(value)


departments_crawling(1)
