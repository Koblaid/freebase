CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    name TEXT, gender TEXT,
    freebase_id TEXT NOT NULL
);

CREATE TABLE parent_child (
    parent_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    FOREIGN KEY(parent_id) REFERENCES person(id),
    FOREIGN KEY(child_id) REFERENCES person(id)
);

CREATE TABLE spouse (
    person1 INTEGER NOT NULL,
    person2 INTEGER NOT NULL,
    FOREIGN KEY(person1) REFERENCES person(id),
    FOREIGN KEY(person2) REFERENCES person(id)
);
