import requests, json, os
import pandas, pyarrow

brasilio_url = 'https://brasil.io/api/v1/dataset/'
slug = 'gastos-diretos/gastos/data/'
brasilio_apikey = '________________________________________'

file_gastos_parquet = './dataset/bronze'
file_gastos_csv = './dataset/raw/gastos-diretos.csv.xz'

def file_gastos_json(page: int = 1):
	return f'./dataset/raw/gastos{ page }.json'

def extract_json(page: int = 1):
	if not (page >= 1):
		return None
	elif os.path.exists(file_gastos_json(page)):
		return False

	url = brasilio_url + slug
	url += f'?page={ page }'
	resp = requests.get(url, headers = {
		'Authorization': f'Token { brasilio_apikey }'
	})
	data = resp.json()

	if 'results' not in data:
		return None

	data = data['results']
	with open(file_gastos_json(page), 'w', encoding = 'utf-8') as file:
		json.dump(data, file, indent = 4)
	return True


def bronze(page: int):
	if page >= 1:
		with open(file_gastos_json(page), 'r') as file:
			gastos = json.load(file)
		dframe = pandas.DataFrame(gastos)
	else:
		dframe = pandas.read_csv(file_gastos_csv)
	dframe.to_parquet(file_gastos_parquet,
		engine = 'pyarrow',  index = True,
		partition_cols = [ 'ano', 'mes' ],
	)

if extract_json(1):
	bronze(1)

