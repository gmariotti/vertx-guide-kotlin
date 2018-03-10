package com.thegmariottiblog

const val SQL_CREATE_PAGE_TABLE = """
    CREATE TABLE page (
        id INTEGER AUTO_INCREMENT,
        name VARCHAR(255) UNIQUE,
        content LONGTEXT,
        PRIMARY KEY (id)
    )
"""

const val SQL_GET_PAGE = "SELECT id, content FROM page WHERE name = ?"
const val SQL_CREATE_PAGE = "INSERT INTO page (name, content) VALUES (?, ?)"
const val SQL_SAVE_PAGE = "UPDATE page SET content = ? WHERE id = ?"
const val SQL_ALL_PAGES = "SELECT name FROM page"
const val SQL_DELETE_PAGE = "DELETE FROM page WHERE id = ?"