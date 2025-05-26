#language: pt

Funcionalidade: Executar a aplicação para gerar o arquivo DAPE

  @GeracaoDapeSemParametro
  Cenario: Gerar o arquivo DAPE para todos os clientes
    Dado que exista o arquivo de clientes "client.csv"
    E que exista o arquivo de apostas "bet.csv"
    E que exista o arquivo de bilhetes "ticket.csv"
    E que exista o arquivo de relacionamento de bilhetes e apostas "ticket_bet.csv"
    Quando a aplicação for executada sem parâmetros
    Então deve ser gerado o arquivo "dape_generated.txt" com o conteúdo exatamente igual ao arquivo "dape_test_full_report.txt"

  @GeracaoDapeComParametro
  Cenario: Gerar o arquivo DAPE para um cliente específico
    Dado que exista o arquivo de clientes "client.csv"
    E que exista o arquivo de apostas "bet.csv"
    E que exista o arquivo de bilhetes "ticket.csv"
    E que exista o arquivo de relacionamento de bilhetes e apostas "ticket_bet.csv"
    Quando a aplicação for executada com o parâmetro investigationClientIdt "1"
    Então deve ser gerado o arquivo "dape_generated.txt" com o conteúdo exatamente igual ao arquivo "dape_test_client_1_report.txt"