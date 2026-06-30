from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo.database import Database

from robot_data_platform.catalog import get_dataset, ingest_dataset, list_datasets
from robot_data_platform.db import get_database


class IngestRequest(BaseModel):
    path: str = Field(..., description="Path to a local CSV file")
    description: str | None = None


def create_app(db: Database | None = None) -> FastAPI:
    app = FastAPI(title="robot-data-platform-mini", version="0.1.0")

    def database_dependency() -> Database:
        return db if db is not None else get_database()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/datasets/{dataset_id}/ingest")
    def ingest(
        dataset_id: str,
        request: IngestRequest,
        database: Database = Depends(database_dependency),
    ) -> dict:
        try:
            dataset = ingest_dataset(
                database,
                dataset_id=dataset_id,
                file_path=request.path,
                description=request.description,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return dataset

    @app.get("/datasets")
    def datasets(database: Database = Depends(database_dependency)) -> list[dict]:
        return list_datasets(database)

    @app.get("/datasets/{dataset_id}")
    def dataset(
        dataset_id: str,
        database: Database = Depends(database_dependency),
    ) -> dict:
        result = get_dataset(database, dataset_id)
        if not result:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return result

    return app


app = create_app()
