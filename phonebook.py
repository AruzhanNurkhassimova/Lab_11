import psycopg2
import csv
from tabulate import tabulate 

conn = psycopg2.connect(host="localhost", user = "postgres", password = "Malkuth/Yesod", port = 5432)

cur = conn.cursor()

# выполняет sql-запрос к базе данных (execute)
cur.execute("""CREATE TABLE IF NOT EXISTS phonebook (
      user_id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      surname VARCHAR(255) NOT NULL, 
      phone VARCHAR(255) NOT NULL

)
""")


cur.execute("""
CREATE OR REPLACE FUNCTION search_by_pattern(pattern TEXT)
RETURNS TABLE (
    user_id INT,
    name TEXT,
    surname TEXT,
    phone TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.user_id,
        p.name::TEXT,
        p.surname::TEXT,
        p.phone::TEXT
    FROM phonebook p
    WHERE p.name ILIKE '%' || pattern || '%'
       OR p.surname ILIKE '%' || pattern || '%'
       OR p.phone ILIKE '%' || pattern || '%';
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE insert_or_update_user(p_name TEXT, p_surname TEXT, p_phone TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM phonebook WHERE name = p_name AND surname = p_surname) THEN
        UPDATE phonebook SET phone = p_phone WHERE name = p_name AND surname = p_surname;
    ELSE
        INSERT INTO phonebook (name, surname, phone) VALUES (p_name, p_surname, p_phone);
    END IF;
END;
$$;
""")

cur.execute("""
DROP PROCEDURE IF EXISTS insert_many_users(TEXT[], TEXT[], TEXT[]);

CREATE OR REPLACE PROCEDURE insert_many_users(
    in_names TEXT[],
    in_surnames TEXT[],
    in_phones TEXT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= array_length(in_names, 1) LOOP
        IF in_phones[i] ~ '^[0-9]{11}$' THEN
            INSERT INTO phonebook(name, surname, phone)
            VALUES (in_names[i], in_surnames[i], in_phones[i]);
        ELSE
            RAISE NOTICE 'Invalid phone: %, Name: %, Surname: %', in_phones[i], in_names[i], in_surnames[i];
        END IF;
        i := i + 1;
    END LOOP;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE FUNCTION get_users_with_pagination(p_limit INT, p_offset INT)
RETURNS TABLE(user_id INT, name TEXT, surname TEXT, phone TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.user_id,
        p.name::TEXT,
        p.surname::TEXT,
        p.phone::TEXT
    FROM phonebook p
    ORDER BY p.user_id
    LIMIT p_limit OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE delete_by_name_or_phone(p_name TEXT DEFAULT NULL, p_phone TEXT DEFAULT NULL)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_name IS NOT NULL THEN
        DELETE FROM phonebook WHERE name = p_name;
    ELSIF p_phone IS NOT NULL THEN
        DELETE FROM phonebook WHERE phone = p_phone;
    END IF;
END;
$$;
""")

def insert_data():
    print('Type "csv" or "con" to choose option between uploading csv file or typing from console: ')
    method = input().lower()
    if method == "con":
        name = input("Name: ")
        surname = input("Surname: ")
        phone = input("Phone: ")
        cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", (name, surname, phone))
    elif method == "csv":
        filepath = input("Enter a file path with proper extension: ")
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", tuple(row))
    conn.commit()

def update_data():
    column = input('Type the name of the column that you want to change: ')
    value = input(f"Enter {column} that you want to change: ")
    new_value = input(f"Enter the new {column}: ")
    cur.execute(f"UPDATE phonebook SET {column} = %s WHERE {column} = %s", (new_value, value))
    conn.commit()

def delete_data():
    phone = input('Type phone number which you want to delete: ')
    cur.execute("DELETE FROM phonebook WHERE phone = %s", (phone,))
    conn.commit()

def query_data():
    column = input("Type the name of the column which will be used for searching data: ")
    value = input(f"Type {column} of the user: ")
    cur.execute(f"SELECT * FROM phonebook WHERE {column} = %s", (value,))
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))

def display_data():
    cur.execute("SELECT * FROM phonebook;")
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))

def search_by_pattern():
    pattern = input("Enter a pattern (part of name/surname/phone): ")
    cur.execute("SELECT * FROM search_by_pattern(%s);", (pattern,))
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))
    conn.commit()

def insert_or_update_user():
    name = input("Name: ")
    surname = input("Surname: ")
    phone = input("Phone: ")
    cur.execute("CALL insert_or_update_user(%s, %s, %s);", (name, surname, phone))
    conn.commit()

def insert_many_users():
    filepath = input("Enter CSV path: ")
    names, surnames, phones = [], [], []
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                names.append(row[0])
                surnames.append(row[1])
                phones.append(row[2])

        cur.execute("CALL insert_many_users(%s, %s, %s);", (names, surnames, phones))
        conn.commit()
        print("Bulk insert operation completed.")
    except FileNotFoundError:
        print("File not found. Please check the path.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_paginated_users():
    limit = int(input("Limit: "))
    offset = int(input("Offset: "))
    cur.execute("SELECT * FROM get_users_with_pagination(%s, %s);", (limit, offset))
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))

def delete_by_name_or_phone():
    mode = input("Delete by name or phone? (name/phone): ").lower()
    if mode == "name":
        name = input("Enter name: ")
        cur.execute("CALL delete_by_name_or_phone(%s, NULL);", (name,))
    elif mode == "phone":
        phone = input("Enter phone: ")
        cur.execute("CALL delete_by_name_or_phone(NULL, %s);", (phone,))
    conn.commit()


while True:
    print("""
    List of the commands:
    1. Type "i" to INSERT data.
    2. Type "u" to UPDATE data.
    3. Type "q" to QUERY data (exact match).
    4. Type "s" to SHOW all data.
    5. Type "d" to DELETE by phone.
    6. Type "f" to FINISH and exit.
    7. Type "p" to search by PATTERN.
    8. Type "m" to INSERT or UPDATE user by name+surname.
    9. Type "b" to BULK insert from CSV with validation.
    10. Type "g" to GET paginated records.
    11. Type "x" to DELETE by name or phone.
    """)

    command = input("Command: ").lower()

    if command == "i":
        insert_data()
    elif command == "u":
        update_data()
    elif command == "q":
        query_data()
    elif command == "s":
        display_data()
    elif command == "d":
        delete_data()
    elif command == "f":
        break
    elif command == "p":
        search_by_pattern()
    elif command == "m":
        insert_or_update_user()
    elif command == "b":
        insert_many_users()
    elif command == "g":
        get_paginated_users()
    elif command == "x":
        delete_by_name_or_phone()

cur.close()
conn.close()