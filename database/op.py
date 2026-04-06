from database.db import get_connection

def create_project(name, version):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO projects (name, version)
        VALUES (?, ?)
        """, (name, version))
        conn.commit()
        return cursor.lastrowid
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_project_by_id(project_id):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT id, name, version, created_at
        FROM projects
        WHERE id = ?
        """, (project_id,))
        return cursor.fetchone()
    finally:
        conn.close()


def get_all_projects():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT id, name, version, created_at
        FROM projects
        ORDER BY id DESC
        """)
        return cursor.fetchall()
    finally:
        conn.close()


def update_project(project_id, name, version):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE projects
        SET name = ?, version = ?
        WHERE id = ?
        """, (name, version, project_id))
        conn.commit()
        return cursor.rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_project(project_id):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        DELETE FROM projects
        WHERE id = ?
        """, (project_id,))
        conn.commit()
        return cursor.rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()