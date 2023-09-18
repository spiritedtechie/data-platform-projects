\c application app_user;

CREATE TABLE products (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    price NUMERIC
);

ALTER TABLE products REPLICA IDENTITY FULL;

INSERT INTO products values (1, 'Book', 5.22);
INSERT INTO products values (2, 'Chair', 25.09);
INSERT INTO products values (3, 'Remote Control', 10.43);

