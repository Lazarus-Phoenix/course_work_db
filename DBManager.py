import sys

import psycopg2
from config import config

class DBManager:
    '''
    Класс для работы с базой данных
    '''

    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            params = config()
            self.conn = psycopg2.connect(
                dbname=params['dbname'],
                user=params['user'],
                password=params['password'],
                host=params['host'],
                port=params['port']
            )

            self.cur = self.conn.cursor()

            print("Подключение к PostgreSQL успешно установлено.")
        except psycopg2.Error as e:
            print(f"Ошибка при подключении к PostgreSQL: {e}")
            sys.exit(1)

    def disconnect(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def get_companies_and_vakancies_count(self):
        '''Получение данных о компаниях и количестве вакансий'''

        if not self.cur:
            self.connect()
        self.cur.execute('SELECT company_name, open_vacancies FROM employers')
        rows = self.cur.fethall #читает все оставшиеся результаты запроса и возвращает их как список кортежей.
        #Результат сохраняется в переменную rows
        self.conn.commit()
        return rows

    def get_all_vakancies(self):
        """Получение всех вакансий"""
        if not self.cur:
            self.connect()
        self.cur.execute('SELECT * FROM vacancies')
        rows = self.cur.fethall  # читает все оставшиеся результаты запроса и возвращает их как список кортежей.
        # Результат сохраняется в переменную rows
        self.conn.commit()
        return rows

    def get_avg_salary(self):
        """Получение средней заработной платы"""

        if not self.cur:
            self.connect()
        self.cur.execute('SELECT ROUND(AVG(payment),2) FROM vacancies WHERE payment IS NOT NULL')
        rows = self.cur.fethall  # читает все оставшиеся результаты запроса и возвращает их как список кортежей.
        # Результат сохраняется в переменную rows
        self.conn.commit()
        return f"Средняя заработная плата среди вакансий, в которых указана зарплата: {rows[0][0]} руб."

    def get_vacancies_with_higher_salary(self):
        """Получение вакансий с окладом выше среднего"""
        if not self.cur:
            self.connect()
        self.cur.execute('SELECT * FROM vacancies WHERE payment > (SELECT AVG(payment) FROM vacancies)')
        rows = self.cur.fetchall()

        self.conn.commit()
        return f"Вакансии с окладом выше среднего: {rows}"

    def get_vacancies_with_keyword(self, keyword):
        """Получение вакансий по ключевому слову"""

        if not self.cur:
            self.connect()
        self.cur.execute(f"SELECT * FROM vacancies WHERE vacancies_name LIKE '%{keyword}%'")
        rows = self.cur.fetchall()

        if not rows:
            return f"Вакансии по ключевому слову '{keyword}': нет"
        self.conn.commit()
        return f"Вакансии по ключевому слову '{keyword}': {rows}"