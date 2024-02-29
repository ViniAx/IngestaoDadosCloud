## Descrição do Projeto

Descrição do Projeto
Este projeto consiste na criação de uma DAG (Directed Acyclic Graph) desenvolvida utilizando o Apache Airflow. A DAG executa as seguintes tarefas:

1- Faz uma solicitação à API da Polygon.io para obter dados financeiros.
2- Transforma os dados recebidos em um DataFrame usando a biblioteca Pandas.
3- Insere os dados em uma tabela no Google BigQuery para análise posterior.

## Polygon.io

Polygon.io é uma plataforma que fornece dados de mercado financeiro em tempo real e histórico. Ela oferece uma API que permite acessar dados sobre ações, opções e criptomoedas, entre outros ativos financeiros. Para o projeto apenas a versão gratuita foi utilizada, possibilitando assim sua aplicação com a finalidade de aprendizado e aprofundamento sobre o uso da ferramenta bem como da API acima mencionada.

## Tecnologias

Para o desenvolvido de toda a estrutura, as seguintes tecnologias foram utilizadas:

- Python
- Apache Airflow
- Google Cloud Platform (GCP)
- BigQuery