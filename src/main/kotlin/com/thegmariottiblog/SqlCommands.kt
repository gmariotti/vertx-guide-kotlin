package com.thegmariottiblog

const val SQL_CREATE_PAGE_TABLE = """
    CREATE TABLE page (
        id INTEGER AUTO_INCREMENT,
        name VARCHAR(255) UNIQUE,
        content LONGTEXT,
        PRIMARY KEY (id)
    )
"""
