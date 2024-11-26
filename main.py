import pandasql as ps
import pandas as pd
import pymysql


dados_conexao = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "caitu_enterprise"
}

"""try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()

    departamentos_df = pd.read_csv('departamentos.csv')
    cargos_df = pd.read_csv('cargos.csv')
    funcionarios_df = pd.read_csv('funcionarios.csv')
    dependentes_df = pd.read_csv('dependentes.csv')
    historico_salarios_df = pd.read_csv('historico_salarios.csv')
    projeto_desenvolvido_df = pd.read_csv('projetos_desenvolvidos.csv')
    recursos_projetos_df = pd.read_csv('recursos_projeto.csv')

    print(departamentos_df.columns)


    for _, row in departamentos_df.iterrows():
        -cursor.execute("""
# INSERT INTO departamentos(id_departamento, nome)
# VALUES ( % s, % s)
""", (row['id_departamento'], row['nome']))

for _, row in cargos_df.iterrows():
    cursor.execute("""
# INSERT INTO cargos (id_cargo, nome, salario_base)
# VALUES (%s, %s, %s)
""", (row['id_cargo'], row['nome'], row['salario_base']))

    for _, row in funcionarios_df.iterrows():
        cursor.execute("""
# INSERT INTO funcionarios (id_funcionario, nome, data_contratacao, id_cargo, id_departamento)
# VALUES (%s, %s, %s, %s, %s)
""", (row['id_funcionario'], row['nome'], row['data_contratacao'], row['id_cargo'], row['id_departamento']))

    for _, row in dependentes_df.iterrows():
        cursor.execute("""
# INSERT INTO dependentes (id_dependente, id_funcionario, nome, idade, relacionamento)
# VALUES (%s, %s, %s, %s, %s)
""", (row['id_dependente'], row['id_funcionario'], row['nome'], row['idade'], row['relacionamento']))

    for _, row in historico_salarios_df.iterrows():
        cursor.execute("""
# INSERT INTO historico_salarios (id_historico, id_funcionario, mes_ano, salario)
# VALUES (%s, %s, %s, %s)
""", (row['id_historico'], row['id_funcionario'], row['mes_ano'], row['salario']))

    for _, row in projeto_desenvolvido_df.iterrows():
        cursor.execute("""
# INSERT INTO projetos_desenvolvidos (id_projeto, nome_projeto, descricao, data_inicio, data_conclusao, #id_funcionario, id_departamento, custo, status, cliente)
# VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (row['id_projeto'], row['nome_projeto'], row['descricao'], row['data_inicio'], row['data_conclusao'], row['id_funcionario'], row['id_departamento'], row['custo'], row['status'], row['cliente']))

    for _, row in recursos_projetos_df.iterrows():
        cursor.execute("""
# INSERT INTO recursos_projeto (id_recurso, id_projeto, descricao_recurso, tipo, quantidade, data_utilizacao, custo_unitario)
# VALUES (%s, %s, %s, %s, %s, %s, %s)
""", (row['id_recurso'], row['id_projeto'], row['descricao_recurso'], row['tipo'], row['quantidade'], row['data_utilizacao'], row['custo_unitario']))

    conexao.commit()
    print("Dados inseridos com sucesso!")

except pymysql.MySQLError as e:
    print(f"Erro ao conectar: {e}")
finally:
    conexao.close()
"""

try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()

    query = """
    SELECT 
        d.nome AS departamento,
        AVG(h.salario) AS media_salario
    FROM 
        projetos_desenvolvidos p
    JOIN 
        funcionarios f ON p.id_funcionario = f.id_funcionario
    JOIN 
        departamentos d ON f.id_departamento = d.id_departamento
    JOIN 
        historico_salarios h ON f.id_funcionario = h.id_funcionario
    WHERE 
        p.status = 'Concluído'
        AND h.mes_ano = (SELECT MAX(h2.mes_ano) FROM historico_salarios h2 WHERE h2.id_funcionario = f.id_funcionario)
    GROUP BY 
        d.nome;
    """

    cursor.execute(query)
    resultados = cursor.fetchall()

    print("Média de salários por departamento (projetos concluídos):")
    for departamento, media_salario in resultados:
        print(f"Departamento: {departamento}, Média Salarial: {media_salario}")

except pymysql.MySQLError as e:
    print(f"Erro ao executar a consulta: {e}")

finally:
    if conexao:
        conexao.close()


print(" ")
print(" ")
print(" ")


# 2. Identificar os três recursos materiais mais usados nos projetos, listando a descrição do recurso
# e a quantidade total usada
try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()

    query2 = """
        SELECT
            tipo,
            descricao_recurso,
            SUM(quantidade) AS total_quantidade,
            COUNT(*) AS total_recursos
        FROM recursos_projeto
        GROUP BY tipo, descricao_recurso
        ORDER BY total_recursos DESC
        LIMIT 3;
    """

    cursor.execute(query2)
    resultados = cursor.fetchall()

    print("Os 3 recursos materiais mais usados:")
    for tipo, descricao_recurso, total_quantidade, total_recursos in resultados:
        print(
            f"Tipo: {tipo}, Descrição: {descricao_recurso}, Total Quantidade: {
                total_quantidade}, Total Recursos: {total_recursos}"
        )

    df = pd.read_sql(query2, conexao)
    resultado_json2 = df.to_json(
        'query_2.json', orient="records", indent=4, force_ascii=False)

    print(resultado_json2)

except pymysql.MySQLError as e:
    print(f"Erro ao executar a consulta: {e}")

finally:
    if conexao:
        conexao.close()

print(" ")
print(" ")
print(" ")

# 3. Calcular o custo total dos projetos por departamento, considerando apenas os projetos
# 'Concluídos'

try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()
    query3 = """
        SELECT 
            d.nome AS nome_departamento,
            SUM(p.custo) AS custo_total_projetos
        FROM projetos_desenvolvidos p
        JOIN departamentos d ON p.id_departamento = d.id_departamento
        WHERE p.status = 'Concluído'
        GROUP BY d.nome;
    """

    cursor.execute(query3)
    resultados = cursor.fetchall()

    print("Custo total dos projetos concluídos por departamento:")
    for nome_departamento, custo_total_projetos in resultados:
        print(f"Departamento: {nome_departamento}, Custo Total: {
              custo_total_projetos}")

    df = pd.read_sql(query3, conexao)
    resultado_json3 = df.to_json(
        'query_3.json', orient="records", indent=4, force_ascii=False)

    print(resultado_json3)

except pymysql.MySQLError as e:
    print(f"Erro ao executar a consulta: {e}")

finally:
    if conexao:
        conexao.close()

print(" ")
print(" ")
print(" ")

# 4. Listar todos os projetos com seus respectivos nomes, custo, data de início, data de conclusão
# e o nome do funcionário responsável, que estejam 'Em Execução'

try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()

    query4 = """
        SELECT
            id_projeto, nome_projeto, custo, data_inicio, data_conclusao, id_funcionario
        FROM projetos_desenvolvidos
        WHERE status = 'Em Execução'
    """

    cursor.execute(query4)
    resultados = cursor.fetchall()

    print("Projetos que estão no status 'Em Execução': ")
    for id_projeto, nome_projeto, custo, data_inicio, data_conclusao, id_funcionario in resultados:
        print(f"ID: {id_projeto}, Nome: {nome_projeto}, Custo: {custo}, "
              f"Data Início: {data_inicio}, Data Conclusão: {data_conclusao}, "
              f"ID Funcionário: {id_funcionario}")

    df = pd.read_sql(query4, conexao)
    resultado_json4 = df.to_json(
        'query_4.json', orient="records", indent=4, force_ascii=False)

    print(resultado_json4)


except pymysql.MySQLError as e:
    print(f"Erro ao executar a consulta: {e}")

finally:
    if conexao:
        conexao.close()

print(" ")
print(" ")
print(" ")

# 5. Identificar o projeto com o maior número de dependentes envolvidos, considerando que os
# dependentes são associados aos funcionários que estão gerenciando os projetos.
try:
    conexao = pymysql.connect(**dados_conexao)
    cursor = conexao.cursor()

    query5 = """
    SELECT 
        p.nome_projeto AS nome_projeto,
        f.nome AS nome_funcionario,
        COUNT(d.id_dependente) AS Total_dependentes
        FROM projetos_desenvolvidos p
        JOIN funcionarios f ON p.id_funcionario = f.id_funcionario
        JOIN dependentes d ON f.id_funcionario = d.id_funcionario

        GROUP BY p.nome_projeto, f.nome
        ORDER BY Total_dependentes DESC
        LIMIT 1;
    """
    cursor.execute(query5)
    resultados = cursor.fetchall()

    for nome_projeto, nome_funcionario, Total_dependentes in resultados:
        print(f"Nome do projeto {nome_projeto}, nome do funcionario {
              nome_funcionario}, Total de depentendes {Total_dependentes} ")

    df = pd.read_sql(query5, conexao)
    resultado_json = df.to_json(
        'query_5.json', orient="records", indent=4, force_ascii=False)

    print(resultado_json)

except pymysql.MySQLError as e:
    print(f"Erro ao executar a consulta: {e}")

finally:
    if conexao:
        conexao.close()
