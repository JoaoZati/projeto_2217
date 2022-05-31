# Analise de dados realizado para a empresa Mitsidi

## Objetivo:
- Conseguir realizar uma analise de dados em datasets robustos em arquvios csv com o tamanho total de 160Gb utilizando Python

## Como Rodar:

### Necessário estar instalado:
- Baixe o Python no seu computador;
- Baixe o git no seu computador;
- instalar a biblioteca pipenv:
- No seu terminal digite:
```
pip install pipenv
```

### Instalar pacotes:
- No seu terminal digite:
```
python -m venv .venv
```
ou "python3 -m venv .venv"
```
pipenv install
```
```
pipenv shell
```
- Assim você terá todas as bibliotecas necessárias para rodar o codigo
- Opcionalmente você pode configurar se VS Code para abrir automaticamente esse ambiente virtual: Aperte Crl+Shift+P, digite Python: Select Interpreter. Selecione o interpretador dentro da pasta ./.venv/bin/python. Caso contrario você terá que abrir manualmente com pipenv shell todas as vezes;

### Setar Variaveis de ambiete:
- Para setar variaveis de ambiente como caminho para ler os arquivos de excel, vá em .env_sample e retire o _sample deixando apenas .env
- Dentro do arquivo .env digite o caminho completo da pasta onde se encontram os arquvivos excell a serem usados (nota no windows voce deve testar usar \\ ao inves de \ porque o programa entende que \ é um caractere especial)
