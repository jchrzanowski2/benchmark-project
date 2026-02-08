CREATE TABLE papers (
    id VARCHAR(255) PRIMARY KEY,
    title TEXT NOT NULL,
    abstract TEXT,
    doi VARCHAR(255),
    submitter VARCHAR(255),
    update_date DATE
);

CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    author_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE paper_authors (
    paper_id VARCHAR(255) REFERENCES papers(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, author_id)
);

CREATE TABLE paper_categories (
    paper_id VARCHAR(255) REFERENCES papers(id) ON DELETE CASCADE,
    category_id INTEGER REFERENCES categories(category_id) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, category_id)
);