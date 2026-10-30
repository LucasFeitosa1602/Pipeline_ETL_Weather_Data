# 🌤️ Pipeline ETL - Dados Climáticos de São Paulo

> Pipeline ETL orquestrado com Airflow que coleta dados meteorológicos de São Paulo a cada hora, transforma com Pandas e armazena em PostgreSQL.

![Python](https://img.shields.io/badge/Python-3.14+-3776AB?style=flat&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.1.7-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-4169E1?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-SEU_USUARIO-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/SEU_USUARIO/)

---

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura](#-arquitetura)
- [Stack Tecnológica](#-stack-tecnológica)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Detalhamento das Etapas](#-detalhamento-das-etapas)
- [A DAG no Airflow](#-a-dag-no-airflow)
- [Créditos](#-créditos)

---

## 🎯 Sobre o Projeto

Projeto prático de **Engenharia de Dados** desenvolvido a partir de uma videoaula (veja os [Créditos](#-créditos)). O objetivo foi construir um pipeline ETL completo, do consumo da API até a persistência em banco de dados, com orquestração e ambiente containerizado.

A cada hora o pipeline:

1. **Extrai** os dados climáticos de São Paulo da API OpenWeatherMap
2. **Transforma** o JSON bruto em uma tabela limpa e padronizada
3. **Carrega** o resultado em uma tabela PostgreSQL, pronta para análises

---

## 🛠️ Stack Tecnológica

**Core**

- **Python 3.14+** - linguagem principal
- **Apache Airflow 3.1.7** - orquestração
- **PostgreSQL 14** - banco de dados relacional
- **Docker & Docker Compose** - containerização

**Bibliotecas Python**

- **pandas** - transformação dos dados
- **requests** - chamadas HTTP à API
- **SQLAlchemy** + **psycopg2** - conexão com o PostgreSQL
- **python-dotenv** - variáveis de ambiente

**Outras ferramentas**

- **Redis** - message broker do Celery
- **Jupyter Notebook** - exploração dos dados
- **uv** - gerenciador de pacotes

---

## 📁 Estrutura do Projeto

```
.
├── dags/                  # DAGs do Airflow
│   └── weather_dag.py
├── src/                   # Código do ETL
│   ├── extract_data.py
│   ├── transform_data.py
│   └── load_data.py
├── notebooks/             # Análises exploratórias
├── plugins/               # Plugins do Airflow (vazio)
├── config/                # .env e airflow.cfg (não versionado)
├── data/                  # JSON/Parquet temporários (não versionado)
├── logs/                  # Logs do Airflow (não versionado)
├── docker-compose.yaml
├── main.py                # Execução local sem Airflow
├── pyproject.toml
├── uv.lock
└── README.md
```

## 🔍 Detalhamento das Etapas

### 📥 Extract — `src/extract_data.py`

- Faz um GET na API do OpenWeatherMap
- Valida o status code (200 = sucesso)
- Salva a resposta bruta em `data/weather_data.json`

Dados coletados: temperatura (atual, mínima, máxima), sensação térmica, umidade, pressão, vento, nuvens, nascer/pôr do sol e coordenadas.

### 🔄 Transform — `src/transform_data.py`

1. Lê o JSON e cria o DataFrame com `pd.json_normalize()`
2. Expande a coluna `weather` (lista de dicionários) em colunas próprias
3. Remove colunas desnecessárias (`weather`, `weather_icon`, `sys.type`)
4. Renomeia colunas para nomes claros (`main.temp` → `temperature`, `coord.lon` → `longitude`, etc.)
5. Converte timestamps Unix para datetime no fuso `America/Sao_Paulo`

```python
df[col] = (
    pd.to_datetime(df[col], unit="s", utc=True)
      .dt.tz_convert("America/Sao_Paulo")
)
```

### 💾 Load — `src/load_data.py`

```python
engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:5432/{database}"
)

df.to_sql(
    name="sp_weather",
    con=engine,
    if_exists="append",
    index=False,
)
```

Após a inserção, um `SELECT COUNT(*)` valida o total de registros e o resultado é registrado em log.

---

## 📊 A DAG no Airflow

Arquivo: `dags/weather_dag.py`

```python
extract() >> transform() >> load()
```

- **Agendamento:** `0 */1 * * *` (de hora em hora)
- **catchup:** `False` (não executa datas passadas)
- **Entre `transform` e `load`** os dados trafegam em **Parquet**, que preserva os tipos (datetime, float) e evita problemas de serialização do Airflow

---

## 🙏 Créditos

Projeto desenvolvido seguindo a videoaula de [@vbluuiza](https://youtube.com/@vbluuiza). Repositório original: [pipeline_etl_weather_data_tutorial_youtube](https://github.com/vbluuiza/pipeline_etl_weather_data_tutorial_youtube).

---
