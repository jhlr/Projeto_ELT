import requests, json, os
import pandas, pyarrow

brasilio_url = 'https://brasil.io/api/v1/dataset/'
slug = 'gastos-diretos/gastos/data/'
brasilio_apikey = '________________________________________'

# partition folder
bronzepath = './dataset/bronze'
silverpath = './dataset/silver'
# tambem baixei o arquivo csv completo para simplificar
gastos_csv = './dataset/raw/gastos-diretos.csv.xz'

bloqueado = 'Detalhamento das informações bloqueado.'
protegido = 'Informações protegidas por sigilo, nos termos da legislação, para garantia da segurança da sociedade e do Estado'

def main():
	n = 25
	for p in range(1, n+1)
		extract_json(p)
	bronze(n)
	silver()
	# gold()

def isblank(s):
	return s in [ 'None', '', None,
		bloqueado, protegido ]

def gastos_json(page: int, mode=None, /):
	# nome do json com o numero da pasta
	fname = f'./dataset/raw/gastos{ page }.json'
	if type(page) is not int or page <= 0: return None
	if mode is None: return fname
	return open(fname, mode, encoding = 'utf-8')

def extract_json(page: int):
	# None se error
	# False se ja adicionado
	if type(page) is not int or page <= 0:
		return None
	elif os.path.exists(gastos_json(page)):
		return False
	url = brasilio_url + slug
	url += f'?page={ page }'
	resp = requests.get(url, headers = {
		'Authorization': f'Token { brasilio_apikey }'
	})
	data = resp.json()
	# algum output estranho do servidor
	if 'results' not in data: return None
	# ignorando cabecalho
	data = data['results']
	with gastos_json(page, 'w') as file:
		json.dump(data, file, ensure_ascii = False, indent = 4)
	return True


def bronze(npages: int):
	dframe = None
	if type(npages) is int and npages >= 1:
		gastos = []
		# lista de entradas
		for p in range(1, npages+1):
			with gastos_json(p, 'r') as file:
				# acrescenta na lista
				gastos.extend(json.load(file))
		dframe = pandas.DataFrame(gastos)
	# opcao de pegar do csv
	else: dframe = pandas.read_csv(gastos_csv)
	# particionado em pastinhas
	dframe.to_parquet( bronzepath,
		engine = 'pyarrow', index = True,
		partition_cols = [ 'ano', 'mes' ],
	)

def silver():
	global dframe
	dframe = pandas.read_parquet(bronzepath)
	for kk in dframe.keys():
		if dframe[kk].apply(isblank).all():
			del dframe[kk]
	dframe = dframe.dropna(subset=['valor'])
	intcols = [ 'ano', 'mes' ] + [ kk
		for kk in dframe.keys()
		if kk.startswith('codigo_')
		and kk != 'codigo_acao' ]

	for kk in dframe.keys():
		t = 'string'
		if kk in intcols: 	t = 'int'
		elif kk == 'valor': t = 'float'
		dframe[kk] = dframe[kk].astype(t)

	dframe.to_parquet(silverpath,
		engine = 'pyarrow', index = True,
		partition_cols = [ 'ano', 'mes' ],
	)

__name__ == "__main__" and main()
