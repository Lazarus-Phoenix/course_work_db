import sys
from typing import List, Tuple, Dict, Any

import psycopg2
from config import config


class DBManager:
    """
    Класс для управления взаимодействием с базой данных PostgreSQL.
    Этот класс предоставляет методы для выполнения различных операций с базой данных,
    включая создание таблиц, вставку данных и получение информации из базы.
    """

    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        """
        Подключается к базе данных PostgreSQL.
        Метод устанавливает соединение с сервером базы данных,
        используя предоставленные параметры подключения.
        Returns: None
        """
        try:
            params = config()
            self.conn = psycopg2.connect(
                dbname=params["dbname"],
                user=params["user"],
                password=params["password"],
                host=params["host"],
                port=params["port"],
            )

            self.cur = self.conn.cursor()

            print("Подключение к PostgreSQL успешно установлено.")
        except psycopg2.Error as e:
            print(f"Ошибка при подключении к PostgreSQL: {e}")
            sys.exit(1)

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        Выполняет SQL-запрос для объединения информации из таблиц employers и vacancies.
        Returns:
        list: Список кортежей (company_name, vacancy_count)
        """
        if not self.cur:
            self.connect()

        try:
            self.cur.execute(
                """
                SELECT e.company_name, COUNT(v.vacancy_id) as vacancy_count 
                FROM employers e
                LEFT JOIN vacancies v ON e.employer_id = v.employer_id
                GROUP BY e.company_name;
                """
            )
            rows = self.cur.fetchall()
            return rows
        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None

    def is_connected(self):
        """
        An artifact of my brainstorming, for you Stalker
        """
        return self.conn is not None and self.cur is not None

    def get_all_vacancies(self) -> List[Dict[str, Any]]:
        """
        Получает полную информацию о всех вакансиях и связанных компаниях.
        Выполняет SQL-запрос для получения информации из таблиц employers и vacancies.
        Returns:
        list: Список словарей, содержащих информацию о вакансиях и компаниях.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                   SELECT 
                       e.company_name,
                       v.vacancy_id,
                       v.vacancies_name,
                       v.payment,
                       v.requirement,
                       v.vacancies_url
                   FROM employers e
                   LEFT JOIN vacancies v ON e.employer_id = v.employer_id;
               """
            )
            return cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None

    def get_avg_salary(self) -> str:
        """
        Получает среднюю заработную плату по всем вакансиям.
        Выполняет SQL-запрос для расчета средней зарплаты.
        Returns:
        str: Строка с результатом расчета средней зарплаты.
        """

        if not self.cur:
            self.connect()
        self.cur.execute(
            "SELECT ROUND(AVG(payment),2) FROM vacancies WHERE payment IS NOT NULL"
        )
        rows = (
            self.cur.fetchall()
        )  # читает все оставшиеся результаты запроса и возвращает их как список кортежей.
        # Результат сохраняется в переменную rows
        self.conn.commit()
        return f"Средняя заработная плата среди вакансий, в которых указана зарплата: {rows[0][0]} руб."

    def get_vacancies_with_higher_salary(self) -> str:
        """
        Получает список всех вакансий с окладом выше среднего.

        Выполняет SQL-запрос для выбора вакансий с более высоким окладом.

        Returns:
            str: Форматированная строка с информацией о вакансиях с выше средней зарплатой.
        """
        if not self.cur:
            self.connect()

        try:
            self.cur.execute('SELECT * FROM vacancies WHERE payment > (SELECT AVG(payment) FROM vacancies)')
            rows = self.cur.fetchall()
            self.conn.commit()

            if not rows:
                return "Вакансии с окладом выше среднего: нет"

            result = "Вакансии с окладом выше среднего:\n"
            for row in rows:
                vacancy_id, vacancies_name, payment, requirement, vacancies_url, employer_id = row
                result += f"ID: {vacancy_id}\n"
                result += f"Название: {vacancies_name}\n"
                result += f"Оклад: {payment} руб.\n"
                result += f"Требования: {requirement}...\n"  # Обрезаем до 100 символов
                result += f"Ссылка: {vacancies_url}\n"
                result += f"ID работодателя: {employer_id}\n"

            return result
        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None

    def get_vacancies_with_keyword(self, keyword):
        """Получение вакансий по ключевому слову"""

        if not self.cur:
            self.connect()
        self.cur.execute(
            f"SELECT * FROM vacancies WHERE LOWER(vacancies_name) LIKE LOWER('%{keyword}%')"
        )  # Все регистры
        rows = self.cur.fetchall()

        if not rows:
            return "Вакансии с окладом выше среднего: нет"

        result = f"Вакансии по ключевому слову '{keyword}':"
        for row in rows:
            vacancy_id, vacancies_name, payment, requirement, vacancies_url, employer_id = row
            result += f"ID: {vacancy_id}\n"
            result += f"Название: {vacancies_name}\n"
            result += f"Оклад: {payment} руб.\n"
            result += f"Требования: {requirement}\n"  # Обрезаем до 50 символов
            result += f"Ссылка: {vacancies_url}\n"
            result += f"ID работодателя: {employer_id}\n"
        return result

    #     if not rows:
    #         return f"Вакансии по ключевому слову '{keyword}': нет"
    #     self.conn.commit()
    #     return f"Вакансии по ключевому слову '{keyword}':{rows}"
    #
    def disconnect(self) -> None:
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
