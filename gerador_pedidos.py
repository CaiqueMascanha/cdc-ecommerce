import time
import random
from faker import Faker
import psycopg2

# Configurações do Banco
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce_db"
DB_USER = "postgres"
DB_PASS = "postgres"

# Inicializa o Faker
fake = Faker('pt_BR')

def conectar_db():
    """Conecta ao banco de dados PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conectado ao PostgreSQL com sucesso!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None

def criar_cliente(cursor):
    """Insere um novo cliente falso no banco."""
    nome = fake.name()
    email = fake.email()
    cursor.execute(
        "INSERT INTO clientes (nome, email) VALUES (%s, %s) RETURNING id",
        (nome, email)
    )
    cliente_id = cursor.fetchone()[0]
    print(f"Novo Cliente: {nome} (ID: {cliente_id})")
    return cliente_id

def criar_pedido(cursor, cliente_id):
    """Insere um novo pedido falso para um cliente."""
    valor = round(random.uniform(30.50, 1500.00), 2)
    cursor.execute(
        "INSERT INTO pedidos (cliente_id, valor_total) VALUES (%s, %s)",
        (cliente_id, valor)
    )
    print(f"  -> Novo Pedido: R$ {valor:.2f} para o cliente ID {cliente_id}")

def main():
    conn = conectar_db()
    if conn is None:
        return

    cursor = conn.cursor()
    
    # Pega IDs de clientes que já existem, se houver
    cursor.execute("SELECT id FROM clientes")
    ids_clientes = [row[0] for row in cursor.fetchall()]

    print("Iniciando simulação... Pressione CTRL+C para parar.")

    try:
        while True:
            # 50% de chance de criar um novo cliente
            if not ids_clientes or random.random() < 0.0:
                novo_id = criar_cliente(cursor)
                ids_clientes.append(novo_id)
            
            # Sempre cria um pedido para um cliente aleatório
            cliente_selecionado = random.choice(ids_clientes)
            criar_pedido(cursor, cliente_selecionado)
            
            # Commit a cada transação
            conn.commit()
            
            # Espera um tempo aleatório
            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print("\nSimulação interrompida.")
    finally:
        cursor.close()
        conn.close()
        print("Conexão com o banco fechada.")

if __name__ == "__main__":
    main()