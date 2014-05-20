CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    name TEXT, gender TEXT,
    freebase_id TEXT NOT NULL,
    UNIQUE(freebase_id)
);

CREATE TABLE parent_child (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    FOREIGN KEY(parent_id) REFERENCES person(id),
    FOREIGN KEY(child_id) REFERENCES person(id),
    UNIQUE(parent_id, child_id)
);

CREATE TABLE spouse (
    person1 INTEGER NOT NULL,
    person2 INTEGER NOT NULL,
    FOREIGN KEY(person1) REFERENCES person(id),
    FOREIGN KEY(person2) REFERENCES person(id),
    UNIQUE(person1, person1)
);

CREATE TABLE family (
    id INTEGER PRIMARY KEY,
    ancestor_id INTEGER NOT NULL,
    max_generation_depth INTEGER NOT NULL,
    person_count INTEGER NOT NULL,
    FOREIGN KEY(ancestor_id) REFERENCES person(id)
);

CREATE TABLE family_member (
    id INTEGER PRIMARY KEY,
    family_id INTEGER NOT NULL,
    parent_child_id INTEGER NOT NULL,
    FOREIGN KEY(family_id) REFERENCES family(id),
    FOREIGN KEY(parent_child_id) REFERENCES parent_child(id),
    UNIQUE(family_id, parent_child_id)
);
