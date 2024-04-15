import psycopg2
import random
import string
import numpy as np
import pandas as pd
import os, errno
from psycopg2 import Error
from psycopg2 import sql
from datetime import datetime, timedelta
from tkinter import *
from tkinter import ttk

#весь класс просто генерирует данные
class Data:


    def __init__(self):
        self.int_numbers = []
        self.words = []
        self.dates = []
        self.float_numbers = []
        self.random_bool = []

    def get_data(self, rows, sub_table = False):
        int_numbers = [random.randint(2, 20) for i in range(rows)]
        words = self.gen_words(rows)
        dates = self.gen_datetime(rows)
        float_numbers = self.gen_float(rows)
        random_bool = self.gen_bool(rows)
        ids = [i+1 for i in range(rows)]
        random.shuffle(ids)
        
        if sub_table:
            return list(zip(ids, words, dates, random_bool, int_numbers, float_numbers))
        else:
            return list(zip(words, dates, random_bool, int_numbers, float_numbers))
    
    def gen_datetime(self, rows, min_year=1950, max_year=datetime.now().year):
        for i in range(rows):
            start = datetime(min_year, 1, 1)
            years = max_year - min_year + 1
            end = start + timedelta(days=365 * years)
            date = start + (end - start) * random.random()
            self.dates.append(datetime.date(date))

        return self.dates
    
    def gen_words(self, rows):
        for i in range(rows):
            length = random.randint(2, 15)
            letters = string.ascii_lowercase
            word = ''.join(random.choice(letters) for i in range(length))
            self.words.append(word)
        
        return self.words
    
    def gen_bool(self, rows):
        for _ in range(rows):
            self.random_bool.append(bool(random.getrandbits(1)))
        
        return self.random_bool
    
    def gen_float(self, rows):
        for _ in range(rows):
            fl_number = round(random.uniform(1, 1000), random.randint(2, 6))
            self.float_numbers.append(fl_number)
            
        return self.float_numbers
    
class SQLconnection:

    def __init__(self):
        self.connection = psycopg2.connect(user="postgres",
                                          password="1234",
                                          host="127.0.0.1",
                                          port="5432",
                                          database="test")
        self.cursor = self.connection.cursor()

    #подкючение к бд
    def db_load(self):
        try:
            self.connection
            
            print("Информация о сервере PostgreSQL")
            print(self.connection.get_dsn_parameters(), "\n")

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        
        finally:
            self.end_connection()

    #вспомогательная функция, завершения соединения
    def end_connection(self):
         if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Соединение с PostgreSQL закрыто")

    #загрузка данных в бд
    def load_data(self):

        #здесь выбирается случайно количество строк базе данных, можно поставить любые значения
        rows = random.randint(150, 200)
        main_insert_query = """ INSERT INTO rock_band (band_name, date_of_foundation, replacements, number_of_members, day_eraned_money) VALUES
                                                (%s,%s,%s,%s,%s);"""
        
        data_main = dataGen.get_data(rows)
        for row in data_main:
            self.cursor.execute(main_insert_query, row)
            self.connection.commit()

        sub1_insert_query = """ INSERT INTO album (group_id, album_name, date_of_writing, on_radio, number_of_songs, eraned_money) VALUES
                                                (%s,%s,%s,%s,%s,%s);"""
        
        data_sub1 = dataGen.get_data(rows, sub_table=True)
        for row in data_sub1:
            self.cursor.execute(sub1_insert_query, row)
            self.connection.commit()

        sub2_insert_query = """ INSERT INTO vokalist (group_id, vokalist_name, date_of_birth, alive, age, height_meters) VALUES
                                                (%s,%s,%s,%s,%s,%s);"""
        
        data_sub2 = dataGen.get_data(rows, sub_table=True)   
        for row in data_sub2:
            self.cursor.execute(sub2_insert_query, row)
            self.connection.commit()

    # получение данных из таблиц
    def get_data(self):

        self.load_data()

        postgreSQL_select_Query = """select rock_band.Id, vokalist.vokalist_name, vokalist.date_of_birth, album.on_radio, vokalist.age, day_eraned_money from rock_band
                                                                    inner join vokalist on rock_band.id = vokalist.group_id
                                                                    inner join album on rock_band.id = album.group_id;"""
        self.cursor.execute(postgreSQL_select_Query)
        all_records = self.cursor.fetchall()
        self.connection.commit() 

        df = pd.DataFrame(data=all_records, columns = ["band_id", "vokalist_name", "date_of_birth", "on_radio", "age", "day_eraned_money"])

        return df

    #очистка sql таблиц        
    def clearSQL(self):

        query_delete = ("delete from album;", "delete from vokalist;", "delete from rock_band;")
        query_restart = ("ALTER SEQUENCE rock_band_id_seq RESTART;", "ALTER SEQUENCE album_id_seq RESTART;", "ALTER SEQUENCE vokalist_id_seq RESTART;")
        #query_update = "UPDATE rock_band SET Id = DEFAULT"

        for i in query_delete:
            self.cursor.execute(i)

        for i in query_restart:
            self.cursor.execute(i)

        self.connection.commit()

class GUI:

    def __init__(self):    
        self.columns =  ("band_id", "vokalist_name", "date_of_birth", "on_radio", "age", "day_eraned_money")

    #cоздание корневого объекта и вызов функций создания GUI
    def create_GUI(self):
        self.table_data = pd.DataFrame()
        self.root = Tk()
        self.root.title("Sber_Test  second task")
        self.root.geometry("1200x600")
        columns = []
        for i in range(6):
            self.frame = Frame(self.root, borderwidth=1)
            columns.append(self.frame)
            
        self.root.grid_rowconfigure(5, weight=1)
        for column, f in enumerate(columns):
            f.grid(row=0, column=column, sticky="nsew")
            self.root.grid_columnconfigure(column, weight=1, uniform="column")
        
        self.create_buttons()
        self.root.mainloop()
        
    #cоздание таблицы значений
    def create_table(self, new_table = False):

        self.table_data = pd.DataFrame()

        if new_table:
            for widget in self.root.winfo_children():
                if isinstance(widget, ttk.Treeview):
                    widget.destroy()

            self.table_data = sql_con.get_data()
            logreg.init_LogReg(self.table_data)
            try:
                self.clear_table()
                self.clear_box()
            except:
                pass

        self.tree = ttk.Treeview(columns=self.columns, show="headings")
        self.tree.grid(row=4, columnspan=6, rowspan=2, sticky="nsew")

        self.tree.heading("band_id", text="id группы", command=lambda: self.sort(0, False))
        self.tree.heading("vokalist_name", text="Имя вокалиста", command=lambda: self.sort(1, False))
        self.tree.heading("date_of_birth", text="Дата рождения", command=lambda: self.sort(2, False))
        self.tree.heading("on_radio", text="На радио", command=lambda: self.sort(3, False))
        self.tree.heading("age", text="Возраст", command=lambda: self.sort(4, False))
        self.tree.heading("day_eraned_money", text="Денег в день", command=lambda: self.sort(5, False))

        for i in range(len(self.table_data.axes[0])):
            self.tree.insert("", END, values=self.table_data.iloc[i,:].tolist())

        self.create_combobox(new_table = True)

    #заполнение таблицы
    def fill_table(self, records):
        for i in range(len(records.axes[0])):
            self.tree.insert("", END, values=records.iloc[i,:].tolist())
    
    #очистка таблицы
    def clear_table(self, on_click = False):
        if on_click:
            self.clear_box()
            self.table_data = pd.DataFrame()
        sql_con.clearSQL()
        self.tree.delete(*self.tree.get_children())

    #cортировка таблицы
    def sort(self, col, reverse):
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        l.sort(key=lambda t: self.tryconvert(t[0], float), reverse=reverse)
        for index,  (_, k) in enumerate(l):
            self.tree.move(k, "", index)
        self.tree.heading(col, command=lambda: self.sort(col, not reverse))

    #вспомогательная функция для сортировки числовых значений
    def tryconvert(self, value, *types):
        for t in types:
            try:
                return t(value)
            except (ValueError, TypeError):
                continue
        return value
    
    #создание комбобоксов для дальнейшей фильтрации
    def create_combobox(self, new_table = False):

        if new_table:
            for widget in self.root.winfo_children():
                if isinstance(widget, ttk.Combobox):
                    widget.destroy()
                    new_table = False

        for col in range(6):
            values = list(self.table_data.iloc[:, col].drop_duplicates())
            values.insert(0, '')
            values_var = StringVar()
            combobox = ttk.Combobox(values=[i for i in values], state="readonly", textvariable=values_var)
            combobox.grid(row=3, column=col, sticky="nsew")
            combobox.bind("<<ComboboxSelected>>", self.selected)

    #обработка фильтра с выбранным значением комбо бокса
    def selected(self, event):
    
        all_selected = []

        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Combobox):
                all_selected.append(widget.get())
        
        df = self.table_data.copy()
        df = df.astype(str)

        for i in range(len(all_selected)):
            if str(all_selected[i]) != '':
                df = df[df.iloc[:, i] == all_selected[i]]
            else:
                continue

        self.clear_table()
        self.fill_table(df)

    #создание кнопок
    def create_buttons(self):

        btn_create_db = ttk.Button(text="Получить новую таблицу", command=lambda: self.create_table(new_table=True))
        btn_create_db.grid(row=1, column=0)

        btn_delete_db = ttk.Button(text="Очистить таблицу", command=lambda: self.clear_table(on_click=True))
        btn_delete_db.grid(row=1, column=1)

        btn_clear = ttk.Button(text="Сбросить фильтры", command=lambda: self.clear_box(filter = True))
        btn_clear.grid(row=1, column=2)


        enabled = IntVar()
        enabled_checkbutton = ttk.Checkbutton(text="Выгрузить с фильтром", variable=enabled)
        enabled_checkbutton.grid(row=1, column=4)

        btn_export = ttk.Button(text="Выгрузить", command=lambda: self.export_data(enabled))
        btn_export.grid(row=1, column=5)


        btn_predict = ttk.Button(text="Сделать предсказания", command=self.get_values)
        btn_predict.grid(row=2, column=0)

        self.label = ttk.Label()
        self.label.grid(row=2, column=3)

        self.entry_age = ttk.Entry()
        self.entry_money = ttk.Entry()
        self.entry_age.grid(row=2, column=4)
        self.entry_money.grid(row=2, column=5)

    def get_values(self):
        try:
            val1 = float(self.entry_age.get())
            val2 = float(self.entry_money.get())
            
            x = pd.DataFrame(np.array([[val1, val2]]), columns = ['a', 'b'])

            self.label['text'] = logreg.predict(x)
        except:
            self.label['text'] = "Данные ввъедены неверно"


    #экспортирование данных в эксель
    def export_data(self, enabled):
        all_values = []
        columns = []
        
        if enabled.get() == 1:
            for line in self.tree.get_children():
                all_values.append(self.tree.item(line)['values'])
                
            for i in range(6):
                heading = self.tree.heading(i)
                columns.append(heading['text'])
                
            df = pd.DataFrame(data=all_values, columns = columns)           
        else:
            df = sql_con.get_data()

        #создаем папку в текущей директории и выгружаем туда эксель
        try:
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exel'))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        
        df.to_excel(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exel/exel.xlsx'), index= False)

    #очистка комбобоксов от значений фильтров
    def clear_box(self, filter = False):
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Combobox):
                widget.set('')
                if not filter:
                    widget.configure(values=[])
        if filter:
            self.fill_table(self.table_data)

class LogReg:

    def init_LogReg(self, df):

        df['age'] = df['age'].astype(float)
        df['day_eraned_money'] = df['day_eraned_money'].astype(float)

        x = df.drop(['band_id', 'vokalist_name', 'on_radio', 'date_of_birth'], axis=1)
        y = df['on_radio'].astype(int)

        self.fit(x, y)

    def sigmoid(self, x):
        return 1/(1 + np.exp(-x))

    def loss_function(self, X, Y, A, m): 
        return -(1/m)*np.sum( Y*np.log(A) + (1-Y)*np.log(1-A))

    def fit(self, X, Y, epochs= 200, lr= 0.01):
        
        X = X.values.T
        Y = Y.values.reshape(1, X.shape[1])
        
        m = X.shape[1]
        n = X.shape[0]
        
        #определение весов
        weights = np.zeros((n,1))
        B = 0
        
        loss_list = []
        
        for epoch in range(epochs):   
            Z = np.dot(weights.T, X) + B
            A = self.sigmoid(Z)
            
            # cost function
            loss = self.loss_function(X, Y, A, m)
            
            #бустинг
            dW = (1/m)*np.dot(A-Y, X.T)
            dB = (1/m)*np.sum(A - Y)   
            
            #переопределение весов
            weights = weights - lr*dW.T
            B = B - lr*dB
            
            #создание списка ошибок
            loss_list.append(loss)
                                   
        self.weights = weights
        self.B = B
        self.loss = loss
    
    def predict(self, X):  
        pred = np.dot(X, self.weights) + self.B
        y_pred = self.sigmoid(pred)
        return [False if y <= 0.5 else True for y in y_pred]


dataGen = Data()
sql_con = SQLconnection()
gui = GUI()
logreg = LogReg()


gui.create_GUI()