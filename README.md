<div align="center" id="top"> 
  <img src ="docs/concatenation-downloaded_from_github-banner_rmariuzzo.svg" alt="Banner do projeto concat"/>

  &#xa0;

  <!-- <a href="https://{{APP}}.netlify.app">Demo</a> -->
</div>

<h1 align="center">concatenation</h1>

<p align="center">
  <img alt="Github top language" src="https://img.shields.io/github/languages/top/rennanbp/concatenation?color=56BEB8">

  <img alt="Github language count" src="https://img.shields.io/github/languages/count/rennanbp/concatenation?color=56BEB8">

  <img alt="Repository size" src="https://img.shields.io/github/repo-size/rennanbp/concatenation?color=56BEB8">

  <img alt="License" src="https://img.shields.io/github/license/rennanbp/concatenation?color=56BEB8">

  <!-- <img alt="Github issues" src="https://img.shields.io/github/issues/rennanbp/concatenation-main?color=56BEB8" /> -->

  <!-- <img alt="Github forks" src="https://img.shields.io/github/forks/rennanbp/concatenation-main?color=56BEB8" /> -->

  <!-- <img alt="Github stars" src="https://img.shields.io/github/stars/rennanbp/concatenation-main?color=56BEB8" /> -->
</p>

<!-- Status -->

<!-- <h4 align="center"> 
	🚧  Concatenation 🚀 Under construction...  🚧
</h4> 

<hr> -->

<p align="center">
  <a href="#dart-sobre">Sobre</a> &#xa0; | &#xa0; 
  <a href="#sparkles-funcionalidades">Funcionalidades</a> &#xa0; | &#xa0;
  <a href="#rocket-tecnologias">Tecnologias</a> &#xa0; | &#xa0;
  <a href="#white_check_mark-pré-requisitos">Pré-requisitos</a> &#xa0; | &#xa0;
  <a href="#checkered_flag-instalação">Instalação</a> &#xa0; | &#xa0;
</p>

<br>

## :dart: Sobre ##

Projeto de concatenação de xlsx de vendas, onde o principal objetivo foi concatenar duas planilhas. Uma nova funcionalidade foi implementada para gerar e visualizar o volume de vendas fictício entre 2020 e 2024. O processo utiliza uma fórmula com o IPCA (IBGE) para simular os dados, e o resultado final é um dashboard de vendas interativo em uma página HTML.

## :sparkles: Funcionalidades ##

:heavy_check_mark: Concatenar planilhas;\
:heavy_check_mark: Manipular e Analisar dados de vendas;\
:heavy_check_mark: Gerar página HTML com dashboard;\
:heavy_check_mark: Obter dados atualizados do IPCA IBGE;\

## :rocket: Tecnologias ##

- [Python](https://www.python.org/) - Principal linguagem de programação
- [Jupyter Notebook](https://jupyter.org/) - Ambiente para executar códigos
- [Git](https://git-scm.com) - Uso no versionamento do código
- Pandas - Aplicação em manipulação e análise de dados
- Openpyxl - Uso para extrair dados de planilhas Excel
- [Docker](https://www.docker.com/) - Uso como ambiente de desenvolvimento em container para a aplicação dashboard_page_generator 
- plotly - Aplicação em visualização de dados
- xlrd - Uso para extrair dados de planilhas Excel
- requests - Uso para download de arquivo remoto 

## :white_check_mark: Pré-requisitos ##

Para baixar o projeto é recomendado usar o Git com git clone.

Para executar o projeto é necessário ter um ambiente com Python e Jupyter Notebook

## :checkered_flag: Instalação ##
É necessário ter um ambiente com Jupyter Notebook.

```bash
# Clone o repositório do projeto
$ git clone https://github.com/rennanbp/concatenation
```

```bash
# Navegue para a pasta do projeto
$ cd concatenation
```

```bash
# Recomendado: Crie um ambiente virtual
python -m venv venv
```

```bash
# Ativando o ambiente virtual

  # No Windows
venv\Scripts\activate

  # No Linux ou macOS
source venv/bin/activate
```

```bash
# Instale as dependências requeridas para executar o projeto
pip install -r requirements.txt
```

Inicie o notebook '.ipynb' e execute o código

&#xa0;

<a href="#top">Back to top</a>
