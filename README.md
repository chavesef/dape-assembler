# DAPE-ASSEMBLER (Declaração de Apostas Esportivas)

A aplicação dape-assembler tem como objetivo realizar a montagem de um documento regulatório por nome
DAPE (Declaração de Apostas Esportivas) a partir de quatro arquivos de entrada obtidos do banco de dados da aplicação
https://github.com/chavesef/dape-api, sendo eles: 
- `bet.csv`: contém os dados de apostas cadastradas no banco de dados.
- `client.csv`: contém os dados de clientes registrados no banco de dados.
- `ticket.csv`: contém os dados de bilhetes de apostas registrados no banco de dados.
- `ticket_bet.csv`: contém os dados que relacionam as apostas existentes com os bilhetes registrados no banco de dados.

A aplicação gera um arquivo no formato .txt que contém as informações definidas nas regras do regulatório, sendo que o arquivo pode ser
gerado considerando o filtro de um cliente específico, no caso de uma investigação, ou de todos os clientes cadastrados no banco de dados,
no caso da geração normal.

## Sumário
- [DAPE-ASSEMBLER (Declaração de Apostas Esportivas)](#dape-assembler-declaração-de-apostas-esportivas)
  - [Ambiente de desenvolvimento](#ambiente-de-desenvolvimento)
    - [Tecnologias Utilizadas](#tecnologias-utilizadas)
    - [Configurando ambiente de desenvolvimento](#configurando-ambiente-de-desenvolvimento)
  - [Utilização](#utilização)
  - [Arquivo gerado](#arquivo-gerado)
  - [Testes](#testes)


## Ambiente de desenvolvimento
O ambiente de desenvolvimento é configurado utilizando o Docker para criar uma instância local do s3
utilizando a imagem `localstack`, onde estarão disponíveis os arquivos de _input_ e _output_ da aplicação.


### Tecnologias Utilizadas
- Framework: Spring Boot
- Versão do Java: 17
- Gerenciamento de Dependências: Gradle
- Containers: Docker

### Configurando ambiente de desenvolvimento

- [Baixar e Instalar Docker](https://docs.docker.com/get-docker/)
- [Instalar Docker Compose](https://docs.docker.com/compose/install/)
- Execute o comando `docker-compose up` para criar uma instância do localstack
- Execute o arquivo `initAWS.sh`, disponível em [initAWS](scripts/initAWS.sh), para inicializar o ambiente do S3 com o bucket
e os arquivos de _input_, disponíveis para consulta em [input](scripts/files).

### Acesso à interface do localstack
Para acessar a interface do localstack, utilize o seguinte link:
[https://app.localstack.cloud/dashboard](https://app.localstack.cloud/dashboard)
Pela interface é possível visualizar se a instância está rodando corretamente, além de visualizar os arquivos que são lidos e gerados pela aplicação
acessando o S3.

## Utilização
A aplicação pode ser executada de duas formas:
- Execução normal sem a utilização de parâmetros, gerando um arquivo de _output_ completo, com as informações de todos,
  os clientes disponíveis no arquivo [client.csv](scripts/files/client.csv).
  - Executar o arquivo [DapeAssemblerApplication.java](src/main/java/com/dape/assembler/DapeAssemblerApplication.java) alterando as configurações 
    de execução, passando como parâmetro o seguinte comando:
  - ```
    --parameters {}
    ```
- Execução com a utilização de parâmetros, gerando um arquivo de _output_ filtrado, com as informações de um cliente específico,
  - Caso de uma investigação, passando o ID do cliente como parâmetro para a aplicação.
  - Executar o arquivo [DapeAssemblerApplication.java](src/main/java/com/dape/assembler/DapeAssemblerApplication.java) alterando as configurações
  de execução, passando como parâmetro o seguinte comando, substituindo `<clientId>` pelo ID do cliente desejado:
  - ```
    --parameters {\"investigationClientIdt\":\"<clientId>\"}
    ```
- Após a execução, é possível obter o arquivo gerado rodando o seguinte comando:
  - ```curl http://localhost:4566/regulatory-dape/output/dape_generated.txt -o dape_generated.txt```

## Arquivo gerado
O arquivo gerado é um arquivo .txt que contém as informações definidas nas regras do regulatório, com o nome `dape_generated.txt` no seguinte formato.
- Para cada cliente é gerado um bloco de informações contendo 4 linhas, sendo elas:
  - A primeira linha contém as informações do cliente e o último registro contém o número de bilhetes de apostas registrados pelo cliente.
  - As linhas 2 e 3 contém as informações dos bilhetes registrados no nome do cliente separadas pelos tipos SIMPLES e MÚLTIPLO, respectivamente.
  - A última linha contém a sumarização dos bilhetes registrados no nome do cliente, com o valor total apostado, média dos valores das _odds_ e valor total proveniente
  dos bilhetes que obtiveram sucesso.

## Testes
A aplicação conta com testes unitários e de integração para garantir o correto funcionamento das regras de negócio.
Caso queira executar um cenário de teste integrado específico, adicione o seguinte parâmetro ao executar o cenário pelo intellij
nas opções de VM:
  ```
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
  ```