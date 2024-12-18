import sys

import psycopg2
import requests


from config import config


def get_vacancies(employer_id):
    """Получение данных по API"""
    params = {"area": 1, "page": 0, "per_page": 10}
    url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
    data_vacancies = requests.get(url, params=params).json()

    vacancies_data = []
    for item in data_vacancies["items"]:
        hh_vacancies = {
            "vacancy_id": int(item["id"]),
            "vacancies_name": item["name"],
            "payment": item["salary"]["from"] if item["salary"] else None,
            "requirement": item["snippet"]["requirement"],
            "vacancies_url": item["alternate_url"],
            "employer_id": employer_id,
        }
        vacancies_data.append(hh_vacancies)

    return vacancies_data if vacancies_data else None


def get_employer(employer_id):
    """Получение данных о работодателей по API"""

    url = f"https://api.hh.ru/employers/{employer_id}"
    data_vacancies = requests.get(url).json()
    hh_company = {
        "employer_id": int(employer_id),
        "company_name": data_vacancies["name"],
        "open_vacancies": data_vacancies["open_vacancies"],
    }

    return hh_company


def create_tables():
    """Создание таблиц"""
    try:
        params = config()
        conn = psycopg2.connect(
            dbname=params["dbname"],
            user=params["user"],
            password=params["password"],
            host=params["host"],
            port=params["port"],
        )

        print("Для создания таблиц подключение к PostgreSQL успешно установлено.")
    except psycopg2.Error as e:
        print(f"Создание таблиц ошибка при подключении к PostgreSQL: {e}")
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS vacancies, employers CASCADE;")

    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
                    CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    company_name varchar(255),
                    open_vacancies INTEGER
                    )"""
        )

        cur.execute(
            """
                    CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancies_name varchar(255),
                    payment INTEGER,
                    requirement TEXT,
                    vacancies_url TEXT,
                    employer_id INTEGER REFERENCES employers(employer_id)
                    )"""
        )
    conn.commit()


def add_to_table(employers_list):
    """ Функция наполнения таблиц БД полученными данными"""
    try:
        params = config()
        conn = psycopg2.connect(
            dbname=params["dbname"],
            user=params["user"],
            password=params["password"],
            host=params["host"],
            port=params["port"],
        )
        print("Подключение к PostgreSQL успешно установлено.")

        with conn.cursor() as cur:

            # Очистка таблиц
            cur.execute("TRUNCATE TABLE employers, vacancies RESTART IDENTITY;")

            # Вставка работодателей
            for employer in employers_list:
                employer_list = get_employer(employer)
                if employer_list is not None:
                    cur.execute(
                        "INSERT INTO employers (employer_id, company_name, open_vacancies) "
                        "VALUES (%s, %s, %s)",
                        (
                            employer_list["employer_id"],
                            employer_list["company_name"],
                            employer_list["open_vacancies"],
                        ),
                    )

            # Вставка вакансий
            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                if vacancy_list is not None:
                    for v in vacancy_list:
                        if v is not None:
                            cur.execute(
                                "INSERT INTO vacancies (vacancy_id, vacancies_name, "
                                "payment, requirement, vacancies_url, employer_id) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (
                                    v["vacancy_id"],
                                    v["vacancies_name"],
                                    v["payment"],
                                    v["requirement"],
                                    v["vacancies_url"],
                                    v["employer_id"],
                                ),
                            )

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Произошла ошибка при добавлении данных в таблицу: {e}")
    return


add_to_table(
    [
        "78638",
        "2180",
        "2748",
        "3529",
        "4716984",
        "5516123",
        "3776",
        "2738360",
        "5331842",
        "1740",
    ]
)
