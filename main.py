import psycopg2


def create_db(conn):
    cur = conn.cursor()
    cur.execute("""
        drop table phones;
        drop table customers;
      """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers(
        client_id INTEGER UNIQUE PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(60) UNIQUE
        );
        """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phones(
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES customers(client_id),
        phone VARCHAR(12) UNIQUE
        );
        """)
    conn.commit()


def add_client(conn, client_id, first_name, last_name, email, phones=None):
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO customers(client_id, first_name, last_name, email) VALUES(%s, %s, %s, %s);
      """, (client_id, first_name, last_name, email))
    conn.commit()
    cur.execute("""
      SELECT * FROM customers;
      """)
    print(f'В базу добавлен клиент - {first_name} {last_name}')

    cur.execute("""
      INSERT INTO phones(client_id, phone) VALUES(%s, %s);
      """, (client_id, phones))
    conn.commit()
    # cur.execute("""
    #   SELECT * FROM phones;
    #   """)
    conn.commit()


def add_phone(conn, client_id, phone):
    cur = conn.cursor()
    cur.execute("""
      UPDATE phones SET phone=%s WHERE client_id=%s;
      """, (phone, client_id))
    print(f'В базу добавлен номер телефона - {phone}')
    conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    cur = conn.cursor()
    cur.execute("""
        SELECT * from customers
        WHERE client_id = %s
        """, (client_id, ))
    res = cur.fetchone()
    if first_name is None:
        first_name = res[1]
    if last_name is None:
        last_name = res[2]
    if email is None:
        email = res[3]
    cur.execute("""
        UPDATE customers
        SET first_name = %s, last_name = %s, email =%s 
        where client_id = %s
        """, (first_name, last_name, email, client_id))
    print(f'Обновлен клиент с id {client_id}')
    conn.commit()

def delete_phone(conn, client_id):
    cur = conn.cursor()
    cur.execute("""
      UPDATE phones SET phone=%s WHERE client_id=%s;
    """, ('Null', client_id))
    print(f'Удален номер телефона для клиента с id {client_id}')
    conn.commit()


def delete_client(conn, client_id):
    cur = conn.cursor()
    cur.execute("""
      DELETE FROM phones WHERE client_id=%s;
    """, (client_id,))
    cur.execute("""
      SELECT * FROM phones;
    """)
    cur = conn.cursor()
    cur.execute("""
      DELETE FROM customers WHERE client_id=%s;
    """, (client_id,))
    print(f'Удален клиент с id {client_id}')
    conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    cur = conn.cursor()
    if first_name is None:
        first_name = '%'
    else:
        first_name = '%' + first_name + '%'
    if last_name is None:
        last_name = '%'
    else:
        last_name = '%' + last_name + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if phone is None:
        cur.execute("""
            SELECT c.client_id, c.first_name, c.last_name, c.email, p.phone FROM customers c
            LEFT JOIN phones p ON c.client_id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s
            """, (first_name, last_name, email))
    else:
        cur.execute("""
            SELECT c.client_id, c.first_name, c.last_name, c.email, p.phone FROM customers c
            LEFT JOIN phones p ON c.client_id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s AND p.phone like %s
            """, (first_name, last_name, email, phone))
    return print(f'Найдены данные в базе: {cur.fetchall()}')

if __name__ == '__main__':
    with psycopg2.connect(database="db", user="postgres", password="password") as conn:
        create_db(conn)
        add_client(conn, 1, 'Anton', 'Shukshin', '@shukshin.ru', '+1')
        add_client(conn, 2, 'Sergei', 'Ryabkov', '@ryabkov.ru', '+2')
        add_client(conn, 3, 'Ivan', 'Ivanov', '@ivanov.ru', '+3')
        add_client(conn, 4, 'Roman', 'Ivanov', '@ovechkin.ru', '+4')
        add_client(conn, 5, 'Anton', 'GBD', '@gbd.ru')
        add_phone(conn, 5, '+5')
        change_client(conn, 4, None,'Ronald', 'Mcdonald')
        delete_phone(conn, 1)
        delete_client(conn, 3)
        find_client(conn, first_name='Anton')
        find_client(conn, last_name='Ronald')
        find_client(conn, email='@gbd.ru')
        find_client(conn, phone='+2')
    conn.close()
