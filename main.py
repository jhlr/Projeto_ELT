import requests, json, os
import pandas, pyarrow

brasilio_url = 'https://brasil.io/api/v1/dataset/'
slug = 'gastos-diretos/gastos/data/'
brasilio_apikey = '________________________________________'

# partition folder
gastos_parquet = './dataset/bronze'
# tambem baixei o arquivo csv completo para simplificar
gastos_csv = './dataset/raw/gastos-diretos.csv.xz'

def gastos_json(page: int):
	# nome do json com o numero da pasta
	if page >= 1:
		return f'./dataset/raw/gastos{ page }.json'

def main():
	n = 1000
	for p in range(1, n+1):
		# salvando os json numerados pelas paginas
		extract_json(p)
	# junta os json num parquet particionado
	bronze(n)

def extract_json(page: int):
	# None se error
	# False se ja adicionado
	if not (page >= 1):
		return None
	elif os.path.exists(gastos_json(page)):
		return False

	url = brasilio_url + slug
	url += f'?page={ page }'
	resp = requests.get(url, headers = {
		'Authorization': f'Token { brasilio_apikey }'
	})
	data = resp.json()

	if 'results' not in data:
		# algum output estranho do servidor
		return None

	# ignorando cabecalho
	data = data['results']

	with open(gastos_json(page), 'w', encoding = 'utf-8') as file:
		# salva
		json.dump(data, file, indent = 4)
	return True


def bronze(npages: int):
	dframe = None
	if npages >= 1:
		gastos = []
		# lista de entradas
		for p in range(1, npages+1):
			with open(gastos_json(p), 'r') as file:
				# acrescenta na lista
				gastos.extend(json.load(file))

		dframe = pandas.DataFrame(gastos)
	else:
		# opcao de pegar do csv
		dframe = pandas.read_csv(gastos_csv)
	# particionado em pastinhas
	dframe.to_parquet(gastos_parquet,
		engine = 'pyarrow', index = True,
		partition_cols = [ 'ano', 'mes' ],
	)

if __name__ == "__main__": main()
