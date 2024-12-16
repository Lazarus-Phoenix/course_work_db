from DBManager import DBManager
from utils import create_tables, add_to_table
from pprint import pprint


if __name__ == '__main__':
    employers_list = [
        "78638",
        "2180",
        "2748",
        "3529",
        "4716984",
        "5516123",
        "3776",
        "2738360",
        "5331842",
        "1740"
    ]
    db = DBManager()
    create_tables()
    add_to_table(employers_list)

    text = ("""Варианты использования:
    1 - получить список всех компаний и количество вакансий у каждой компании.
    2 - получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
    3 - получить среднюю зарплату по вакансиям.
    4 - получить список всех вакансий с зарплатой выше средней.
    5 - получить список всех вакансий по ключевому слову.
    6 - показать варианты использования
    7 - выход""")

    print(text)

    while True:
        answer = input("Ваш выбор: ")
        try:
            answer = int(answer)
        except ValueError:
            print("Такого варианта нет")
            continue
        if answer == 1:
            pprint(db.get_companies_and_vacancies_count())
        elif answer == 2:
            pprint(db.get_all_vacancies())
        elif answer == 3:
            pprint(db.get_avg_salary())
        elif answer == 4:
            pprint(db.get_vacancies_with_higher_salary())
        elif answer == 5:
            keyword = input("Введите ключевое слово: ")
            pprint(db.get_vacancies_with_keyword(keyword))
        elif answer == 6:
            print(text)
            continue
        elif answer == 7:
            print("Выход")
            db.disconnect()
            break
        else:
            print("Такого варианта нет")
            continue