import sys
import argparse
from pathlib import Path
import datetime
import cx_Oracle

cx_Oracle.init_oracle_client(lib_dir=r"D:\Down\oracle\instantclient_21_3")

# Аргументы для скрипта
parser = argparse.ArgumentParser()
parser.add_argument('--dir', type = Path)
parser.add_argument('--file', type = Path)
parser.add_argument('--script', type = Path)
parser.add_argument('--all', default = False, type = bool)

ar = parser.parse_args()

xml_encoding = 'cp1251' 
script_encoding = 'utf-8' 

# Функция создания последовательности для автоматической нумерации BUFFER_ID
def create_seq(cursor, connection):	
	print('Попытка создать последовательность для id...', end = ' ')
	cursor.execute('create sequence buffer_id_seq start with 1 increment by 1')
	connection.commit()
	print('Успешно')


# Подключение к базе
try:
	connstr = 'test/111@localhost:1521/orcl'
	connection = cx_Oracle.connect(connstr)
	cr = connection.cursor()
	print('Соединение с базой данных успешно установлено.')
	
	#create_seq(cr, connection)  # для первого запуска, после необходимо закоментировать
	
	if ar.file: 
		if not ar.file.is_file():
			print(f'Ошибка. Путь «{ar.file}» не является верным путём файла.')
			sys.exit(0)
		to_process = [ar.file]
	else: 
		if not ar.dir: 
			ar.dir = Path(input('Укажите путь до папки с XML файлами:\n'))
		if not ar.dir.is_dir(): 
			print(f'Ошибка. Путь «{ar.dir}» не является верным путём папки.')
			sys.exit(0)

		# Поиск в директории
				
		file_list = sorted(ar.dir.glob('**/*.xml')) 
		file_count = len(file_list)

		# Ввод папки и файла(ов)

		if file_count < 1:
			print('Файлы не найдены')
			sys.exit()
		else:
			if ar.all: 
				to_process = file_list
			else:
				max_digits = len(str(file_count))
				
				for num, filep in enumerate(file_list, 1):
					
					print(f'{num:{max_digits}}) {filep}')
												
				while True:
										
					comm = input('Введите номер файла, «a» — чтобы выбрать все, «e» — чтобы выйти:\n').strip()
					if comm and comm in 'eе': 
						sys.exit(0)
					elif comm and comm in 'aа': 
						to_process = file_list
						break
					else:
						try:
							num = int(comm)
							if not 1 <= num <= file_count:
								raise ValueError()
							to_process = [file_list[num - 1]]
							break
						except ValueError as e:
							print(f'Ошибка... Введите число между 1 и {file_count}, a или e.')

	
	print('Обработка файлов...')

	# Запись выбранных файлов в базу
	for fp in to_process:
		print(fp) 
		
		# дата для вставки
		date = datetime.date.today()
		
		with fp.open('r', encoding = xml_encoding) as f:			
			content = f.read() # считываем файл
		
		source_type = 'FILE';
		# Записываем файлы в таблицу. Используем созданную последовательность buffer_id_seq для автоматической нумерации
		cr.execute("insert into BUFFER(BUFFER_ID, XML_CONTENT, DATA_S, TYPE) values(buffer_id_seq.nextval, :xml_content, :load_date, :source_type)",
			{
				'xml_content': content,
				'load_date': date,
				'source_type': source_type
			}
		)
	# записываем изменения
	connection.commit()
	print('Добавление всех файлов успешно завершено.')
	
	
	if not ar.script:
		ar.script = Path(input('Введите путь скрипта для запуска:\n'))
	if not ar.script.is_file():
		print(f'Ошибка. Файл «{ar.script}» не существует.')
		sys.exit(0)
	
	# Запуск скрипта
	with ar.script.open(encoding = script_encoding) as sf:
		sc = sf.read()
		cr.execute(sc)
		connection.commit()

except Exception as e:
	print(e)
finally: 
	if 'cr' in globals():
		cr.close()
	if 'connection' in globals():
		connection.close()

