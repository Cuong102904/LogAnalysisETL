from __future__ import annotations

import os

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

SUPERSET_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
SUPERSET_TRINO_SQLALCHEMY_URI = os.environ.get(
    "SUPERSET_TRINO_SQLALCHEMY_URI",
    "trino://superset@trino:8080/delta/mooc",
)
SUPERSET_TRINO_DATABASE_NAME = os.environ.get("SUPERSET_TRINO_DATABASE_NAME", "trino_delta_mooc")
SUPERSET_TRINO_VALIDATE_TABLES = os.environ.get("SUPERSET_TRINO_VALIDATE_TABLES", "true").lower() == "true"


def main() -> None:
    if not SUPERSET_DATABASE_URI:
        raise RuntimeError("SQLALCHEMY_DATABASE_URI is required")

    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.extensions import db
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=SUPERSET_TRINO_DATABASE_NAME).one_or_none()
        if database is None:
            database = Database(database_name=SUPERSET_TRINO_DATABASE_NAME)
            db.session.add(database)

        database.sqlalchemy_uri = SUPERSET_TRINO_SQLALCHEMY_URI
        database.expose_in_sqllab = True
        database.allow_file_upload = False
        database.allow_ctas = False
        database.allow_cvas = False
        database.allow_dml = False
        database.impersonate_user = False
        db.session.commit()

    validate_trino_database()
    print(
        "Superset bootstrap completed successfully: "
        f"registered {SUPERSET_TRINO_DATABASE_NAME} -> {SUPERSET_TRINO_SQLALCHEMY_URI}"
    )


def validate_trino_database() -> None:
    if not SUPERSET_TRINO_VALIDATE_TABLES:
        return

    engine = create_engine(SUPERSET_TRINO_SQLALCHEMY_URI)
    try:
        with engine.connect() as connection:
            tables = [row[0] for row in connection.execute(text("SHOW TABLES FROM delta.mooc"))]
            for table in tables:
                connection.execute(text(f"SELECT 1 FROM delta.mooc.{table} LIMIT 1"))
            print(f"Validated query access for {len(tables)} registered tables")
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Failed to validate Trino database URI {SUPERSET_TRINO_SQLALCHEMY_URI}: {exc}") from exc
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
